import ray
import ray.local_scheduler as local_scheduler

# store access to nodes by their global coordinate
@ray.remote
class MasterStore:

    def __init__(self):
        self.referenceStore = {"dna":{}, "rna":{}, "individuals":{}}

    def addGraph(self, datatype):
        self.referenceStore[datatype] = {}

    def updateRef(self, datatype, key, value):
        oid = value

        if not isinstance(oid, local_scheduler.ObjectID):
            oid = ray.put(value)        

        self.referenceStore[datatype][key] = oid

    def getRef(self, datatype, key):
        ref = self.referenceStore[datatype][key]
        # check if this is a ray object id or a node
        if isinstance(ref, local_scheduler.ObjectID):
            return ray.get(ref)
        elif isinstance(ref, Node):
            return ref
        else:
            raise ValueError("The graph does not contain references or nodes at key: \'" + key + "\'")

    def refExists(self, datatype, key):
        return datatype in self.referenceStore and key in self.referenceStore[datatype]

class Neighbor:

    @staticmethod
    def opposite():
        return Neighbor

    def __init__(self):
        self.destination_coordinates = set()

    def add(self, coordinate):
        self.destination_coordinates.add(coordinate)

class Previous(Neighbor):

    @staticmethod
    def opposite():
        return Next

    def __init__(self):
        self.destination_coordinates = set()

class Next(Neighbor):

    @staticmethod
    def opposite():
        return Previous

    def __init__(self):
        self.destination_coordinates = set()

# a generic node.
class Node:

    def __init__(self, data, datatype, new_neighbors = {}):
        self.data = data
        self.datatype = datatype
        self.neighbors = { Neighbor: Neighbor(), Previous: Previous(), Next: Next() }
        for neighbor_type in new_neighbors:
            for neighbor in new_neighbors[neighbor_type]:
                self.addNeighbor(neighbor, neighbor_type)
        self.interGraphLinks = {}

    def addNeighbor(self, newNeighbor, type_of_neighbor = Neighbor):
        assert(type(type_of_neighbor).__name__ == 'classobj')
        type_of_neighbor = eval(type_of_neighbor.__name__)
        if type_of_neighbor is Neighbor or type_of_neighbor is Previous or type_of_neighbor is Next:
            self.neighbors[type_of_neighbor].add(newNeighbor)
        else:
            raise ValueError("The type of neighbor must be Neighbor, Previous, or Next")

    def dropNeighbor(self, oldNeighbor):
        self.neighbors.remove(oldNeighbor)

    def addInterGraphLink(self, datatype, key):
        if datatype in self.interGraphLinks:
            if key not in self.interGraphLinks[datatype]:
                self.interGraphLinks[datatype].append(key)
        else:
            self.interGraphLinks[datatype] = [key]

    def merge(self, otherNode):
        for neighbor_type in otherNode.neighbors:
            for neighbor in otherNode.neighbors[neighbor_type].destination_coordinates:
                self.addNeighbor(neighbor, neighbor_type)

        for key, value in otherNode.interGraphLinks.items():
            for link in value:
                self.addInterGraphLink(key, link)

def buildIndividualsGraph(individuals, masterStore):
    for indivID, data in individuals.items():
        node = Node(data, "individuals")
        masterStore.updateRef.remote("individuals", indivID, node)

def buildDnaGraph(referenceGenome, dnaTestData, masterStore):
    # start building the graph
    for i in range(len(referenceGenome)):
        # store the coordinates of neighboring nodes
        neighbors = {}

        if i != 0:
            neighbors[Previous] = set([float(i - 1)])
        if i != len(referenceGenome) - 1:
            neighbors[Next] = set([float(i + 1)])
        
        # create a new node
        node = Node(referenceGenome[i], "dna", neighbors)

        # store a link to the object in the masterStore
        masterStore.updateRef.remote("dna", float(i), node)

    for indiv in dnaTestData:
        for variant in indiv["dnaData"]:
            # store the coordinates of neighboring nodes
            neighbors = {}
            neighbors[Previous] = set([float(int(variant["coordinateStart"]) - 1)])
            neighbors[Next] = set([float(int(variant["coordinateStop"]))])

            # create a new node for the individual data
            node = Node(variant["variantAllele"], 
                        "dna",
                        neighbors)

            node.addInterGraphLink("individuals", indiv["individualID"])

            if ray.get(masterStore.refExists.remote("dna", variant["coordinateStart"])):
                node.merge(ray.get(masterStore.getRef.remote("dna", variant["coordinateStart"])))

            for neighbor_type in neighbors:
                for neighbor in neighbors[neighbor_type]:
                    if ray.get(masterStore.refExists.remote("dna", neighbor)):
                        tempNode = ray.get(masterStore.getRef.remote("dna", neighbor))
                        tempNode.addNeighbor(variant["coordinateStart"], neighbor_type.opposite())
                        masterStore.updateRef.remote("dna", neighbor, tempNode)

            indivNode = ray.get(masterStore.getRef.remote("individuals", indiv["individualID"]))
            indivNode.addInterGraphLink("dna", variant["coordinateStart"])
            masterStore.updateRef.remote("individuals", indiv["individualID"], indivNode)

            masterStore.updateRef.remote("dna", variant["coordinateStart"], node)

def bfs(graphName, startNodeID, masterStore):
    q = [startNodeID]
    visited = []

    while(q and ray.get(masterStore.refExists.remote(graphName, q[0]))):
        currentKey = q[0]
        q.remove(currentKey)
        visited.append(currentKey)

        node = ray.get(masterStore.getRef.remote(graphName, currentKey))
        print(str(currentKey) + "\t" + str(node.data) + "\t" + str(node.interGraphLinks))

        for type_of_neighbor in node.neighbors:
            for neighbor in node.neighbors[type_of_neighbor].destination_coordinates:
                if neighbor not in visited and neighbor not in q:
                    q.append(neighbor)

if __name__ == "__main__":
    ray.init()
    # all communication to adjacent nodes goes through the master store
    masterStore = MasterStore.remote()

    # sample reference genome
    referenceGenome = "CAGTCCTAGCTACGCTCTATCCTCTCAGAGGACCGATCGATATACGCGTGAAACTAGTGCACTAGACTCGAACTGA"

    # sample test data for DNA operations
    dnaTestData = [{"individualID":0, "dnaData":
                    [{"coordinateStart":7.1, "coordinateStop":8.0, "variantAllele": "C"},
                     {"coordinateStart":12.2, "coordinateStop":13.0, "variantAllele": "T"},
                     {"coordinateStart":26.2222, "coordinateStop":27.0, "variantAllele": "TTTT"}]},
                   {"individualID":1, "dnaData":
                    [{"coordinateStart":7.2, "coordinateStop":8.0, "variantAllele": "G"},
                     {"coordinateStart":12.2, "coordinateStop":13.0, "variantAllele": "T"}]}]

    # individual IDs
    individuals = {0: {"Name":"John Doe", "Gender":"M"}, 1: {"Name":"Jane Doe", "Gender":"M"}}

    buildIndividualsGraph(individuals, masterStore)

    # build the graph
    buildDnaGraph(referenceGenome, dnaTestData, masterStore)

    # traverse our new graph to look at
    bfs("dna", 0.0, masterStore)

    print(ray.get(masterStore.getRef.remote("individuals", 0).interGraphLinks))
    print(ray.get(masterStore.getRef.remote("individuals", 1).interGraphLinks))

    print(ray.get(masterStore.getRef.remote("individuals", 0)))
    print(ray.get(masterStore.getRef.remote("individuals", 1)))

    # this will store all reads in their original form
    masterStore.addGraph.remote("reads")
    # this will store the genome graph for all reads
    masterStore.addGraph.remote("readsGenomeGraph")

    #sample reads
    sampleReadData = [{"contigName": "chr1", "start": 268051, "end": 268101, "mapq": 0, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:2307:5603:121126", "sequence": "GGAGTGGGGGCAGCTACGTCCTCTCTTGAGCTACAGCAGATTCACTCNCT", "qual": "BCCFDDFFHHHHHJJJIJJJJJJIIIJIGJJJJJJJJJIIJJJJIJJ###", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": False, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "47T0G1", "origQual": None, "attributes": "XT:A:R\tXO:i:0\tXM:i:2\tNM:i:2\tXG:i:0\tXA:Z:chr16,-90215399,50M,2;chr6,-170736451,50M,2;chr8,+71177,50M,3;chr1,+586206,50M,3;chr1,+357434,50M,3;chr5,-181462910,50M,3;chr17,-83229095,50M,3;\tX1:i:5\tX0:i:3", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None},
                        {"contigName": "chr1", "start": 1424219, "end": 1424269, "mapq": 37, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:2107:15569:102571", "sequence": "AGCGCTGTAGGGACACTGCAGGGAGGCCTCTGCTGCCCTGCTAGATGTCA", "qual": "CCCFFFFFHHHHHJJJJJJJJJJIJJJJJJJJJJJJJJJIJJIJHIIGHI", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": False, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "50", "origQual": None, "attributes": "XT:A:U\tXO:i:0\tXM:i:0\tNM:i:0\tXG:i:0\tX1:i:0\tX0:i:1", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None},
                        {"contigName": "chr1", "start": 1443674, "end": 1443724, "mapq": 0, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:2103:19714:5712", "sequence": "TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT", "qual": "############################BBBCDEEA<?:FDCADDD?;=+", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": True, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "50", "origQual": None, "attributes": "XT:A:R\tXO:i:0\tXM:i:0\tNM:i:0\tXG:i:0\tX0:i:1406", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None},
                        {"contigName": "chr1", "start": 1443676, "end": 1443726, "mapq": 0, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:2103:21028:126413", "sequence": "TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT", "qual": "###########################B?;;AFHFIGDDHDDDDDBD@?=", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": True, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "50", "origQual": None, "attributes": "XT:A:R\tXO:i:0\tXM:i:0\tNM:i:0\tXG:i:0\tX0:i:1406", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None},
                        {"contigName": "chr1", "start": 2653642, "end": 2653692, "mapq": 25, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:2306:20003:84408", "sequence": "ANNACACCCCCAGGCGAGCATCTGACAGCCTGGAACAGCACCCACACCCC", "qual": "######JJJJJJJIJIJJIHGGGIIJJJJJJJJJJJJHHFHHDDDBFC@@", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": True, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "0T0C0C47", "origQual": None, "attributes": "XT:A:U\tXO:i:0\tXM:i:3\tNM:i:3\tXG:i:0\tX1:i:0\tX0:i:1", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None},
                        {"contigName": "chr1", "start": 2664732, "end": 2664782, "mapq": 0, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:2106:12935:169714", "sequence": "GAGCATGTGACAGCCTAGGTCGGCACCCACACCCCCAGGTGAGCATCTGA", "qual": "FDBDCHFFEHDCCAFHIHA6EGB?8GGFF?8IEHEB@FHDHGEDDBD@@@", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": True, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "6C9G33", "origQual": None, "attributes": "XT:A:R\tXO:i:0\tXM:i:2\tNM:i:2\tXG:i:0\tX1:i:13\tX0:i:5", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None},
                        {"contigName": "chr1", "start": 2683541, "end": 2683591, "mapq": 0, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:2107:5053:12847", "sequence": "AGCACCCACAACCACAGGTGAGCATCCGACAGCCTGGAACAGCACCCACA", "qual": "CCCFFFFFHGHHHJIJJJHGGIIJJJJJIJGIIJJIJJIJJJJIJIIJJJ", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": False, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "50", "origQual": None, "attributes": "XT:A:R\tXO:i:0\tXM:i:0\tNM:i:0\tXG:i:0\tXA:Z:chr1,+2687435,50M,0;chr1,+2694861,50M,0;chr1,+2755813,50M,1;\tX1:i:1\tX0:i:3", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None},
                        {"contigName": "chr1", "start": 2689861, "end": 2689911, "mapq": 0, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:2108:5080:115408", "sequence": "GGTGAGCATCTGACAGCCCGGAGCAGCACGCAAACCCCCAGGTGAGCATC", "qual": "@@BFBBDFHHHHGJIJGIIFIEIJJJJIJJJJJJJJJJJJIJGHHICEHH", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": False, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "18T3A27", "origQual": None, "attributes": "XT:A:R\tXO:i:0\tXM:i:2\tNM:i:2\tXG:i:0\tX1:i:21\tX0:i:2", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None},
                        {"contigName": "chr1", "start": 2750194, "end": 2750244, "mapq": 0, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:1204:10966:151563", "sequence": "CCCCCNCACCCCCAGGTGAGCATCTGATGGTCTGGAGCAGCACCCACACC", "qual": "######F;JJJJJJJJJJJJIIIJIJJJJFJJIJJGJHHHHHFFDDD?BB", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": True, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "1A3A12C31", "origQual": None, "attributes": "XT:A:R\tXO:i:0\tXM:i:3\tNM:i:3\tXG:i:0\tXA:Z:chr1,-2653118,50M,3;chr1,-2652838,50M,3;chr1,-2653681,50M,3;chr1,-2694823,50M,3;chr1,-2687397,50M,3;chr1,-2755775,50M,3;chr1,-2653921,50M,3;\tX1:i:0\tX0:i:8", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None},
                        {"contigName": "chr1", "start": 3052271, "end": 3052321, "mapq": 25, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:2107:21352:43370", "sequence": "TCANTCATCTTCCATCCATCCGTCCAACAACCATTTGTTGATCATCTCTC", "qual": "@@<#4AD?ACDCDHGIDA>C?<A;8CBEEBAG1D?BG?GH?@DEHFG@FH", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": False, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "3C44A0T0", "origQual": None, "attributes": "XT:A:U\tXO:i:0\tXM:i:3\tNM:i:3\tXG:i:0\tX1:i:0\tX0:i:1", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None}]

    for read in sampleReadData:
        masterStore.updateRef.remote("reads", read["readName"], read)
        for index in range(len(read["sequence"])):
            data = [read["readName"]]

            neighbors = {}
            if index != 0:
                neighbors[Previous] = read["contigName"] + "\t" + str(read["start"] + index - 1)
            if index != len(read["sequence"]) - 1:
                neighbors[Next] = read["contigName"] + "\t" + str(read["start"] + index + 1)

            node = Node(data, "readsGenomeGraph", neighbors)

            coordinate = read["contigName"] + "\t" + str(read["start"] + index)

            if ray.get(masterStore.refExists.remote("readsGenomeGraph", coordinate)):
                previousNode = ray.get(masterStore.getRef.remote("readsGenomeGraph", coordinate))
                node.merge(previousNode)
                node.data += previousNode.data

            node.addInterGraphLink("reads", read["readName"])
            masterStore.updateRef.remote("readsGenomeGraph", coordinate, node)


    # for storing the feature data
    masterStore.addGraph.remote("features")

    sampleFeatures = [{"featureName": "0","contigName": "chr1", "start": 45520936, "end": 45522463, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "878", "thickStart": "482.182760214932", "thickEnd": "-1"}},
                        {"featureName": "1", "contigName": "chr1", "start": 88891087, "end": 88891875, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "423", "thickStart": "446.01797654123", "thickEnd": "-1"}},
                        {"featureName": "2", "contigName": "chr1", "start": 181088138, "end": 181090451, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "626", "thickStart": "444.771802710521", "thickEnd": "-1"}},
                        {"featureName": "3", "contigName": "chr1", "start": 179954184, "end": 179955452, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "647", "thickStart": "440.10466093652", "thickEnd": "-1"}},
                        {"featureName": "4", "contigName": "chr1", "start": 246931401, "end": 246932507, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "423", "thickStart": "436.374938660247", "thickEnd": "-1"}},
                        {"featureName": "5", "contigName": "chr1", "start": 28580676, "end": 28582443, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "1106", "thickStart": "434.111845970505", "thickEnd": "-1"}},
                        {"featureName": "6", "contigName": "chr1", "start": 23691459, "end": 23692369, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "421", "thickStart": "426.055504846001", "thickEnd": "-1"}},
                        {"featureName": "7", "contigName": "chr1", "start": 201955033, "end": 201956082, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "522", "thickStart": "423.882565088207", "thickEnd": "-1"}},
                        {"featureName": "8", "contigName": "chr1", "start": 207321011, "end": 207323021, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "741", "thickStart": "423.625988483304", "thickEnd": "-1"}},
                        {"featureName": "9","contigName": "chr1", "start": 145520936, "end": 145522463, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "878", "thickStart": "482.182760214932", "thickEnd": "-1"}},
                        {"featureName": "10", "contigName": "chr1", "start": 188891087, "end": 188891875, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "423", "thickStart": "446.01797654123", "thickEnd": "-1"}},
                        {"featureName": "11", "contigName": "chr1", "start": 1181088138, "end": 1181090451, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "626", "thickStart": "444.771802710521", "thickEnd": "-1"}},
                        {"featureName": "12", "contigName": "chr1", "start": 1179954184, "end": 1179955452, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "647", "thickStart": "440.10466093652", "thickEnd": "-1"}},
                        {"featureName": "13", "contigName": "chr1", "start": 1246931401, "end": 1246932507, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "423", "thickStart": "436.374938660247", "thickEnd": "-1"}},
                        {"featureName": "14", "contigName": "chr1", "start": 128580676, "end": 128582443, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "1106", "thickStart": "434.111845970505", "thickEnd": "-1"}},
                        {"featureName": "15", "contigName": "chr1", "start": 123691459, "end": 123692369, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "421", "thickStart": "426.055504846001", "thickEnd": "-1"}},
                        {"featureName": "16", "contigName": "chr1", "start": 1201955033, "end": 1201956082, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "522", "thickStart": "423.882565088207", "thickEnd": "-1"}},
                        {"featureName": "17", "contigName": "chr1", "start": 1207321011, "end": 1207323021, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "741", "thickStart": "423.625988483304", "thickEnd": "-1"}},
                        {"featureName": "18", "contigName": "chr1", "start": 1110963118, "end": 1110964762, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "758", "thickStart": "421.056761458099", "thickEnd": "-1"}}]

    for feature in sampleFeatures:
        node = Node(feature, "features", set())
        for index in range(feature["end"] - feature["start"]):
            coordinate = feature["contigName"] + "\t" + str(read["start"] + index)
            node.addInterGraphLink("readsGenomeGraph", coordinate)
            if ray.get(masterStore.refExists.remote("readsGenomeGraph", coordinate)):
                previousNode = ray.get(masterStore.getRef.remote("readsGenomeGraph", coordinate))
                previousNode.addInterGraphLink("features", feature["featureName"])
                masterStore.updateRef.remote("readsGenomeGraph", coordinate, previousNode)

        masterStore.updateRef.remote("features", feature["featureName"], node)

