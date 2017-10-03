import ray

# store access to nodes by their global coordinate
class MasterStore:
    referenceStore = {"dna":{}, "rna":{}, "individuals":{}}
    
    def addGraph(self, datatype):
        referenceStore[datatype] = {}
    
    def updateRef(self, datatype, key, value):
        self.referenceStore[datatype][key] = value
        
    def getRef(self, datatype, key):
        return self.referenceStore[datatype][key]
    
    def refExists(self, datatype, key):
        return datatype in self.referenceStore and key in self.referenceStore[datatype]

# a generic node.
class Node:
    def __init__(self, data, datatype, neighbors = set(), interGraphLinks = {}):
        self.data = data
        self.datatype = datatype
        self.neighbors = neighbors
        self.interGraphLinks = {}
        
    def addNeighbor(self, newNeighbor):
        self.neighbors.add(newNeighbor)
        
    def dropNeighbor(self, oldNeighbor):
        self.neighbors.remove(oldNeighbor)

    def addInterGraphLink(self, datatype, key):
        if datatype in self.interGraphLinks:
            if key not in self.interGraphLinks[datatype]:
                self.interGraphLinks[datatype].append(key)
        else:
            self.interGraphLinks[datatype] = [key]
    
    def merge(self, otherNode):
        for i in otherNode.neighbors:
            self.addNeighbor(i)
        
        for key, value in otherNode.interGraphLinks.items():
            for link in value:
                self.addInterGraphLink(key, link)

def buildIndividualsGraph(individuals, masterStore):
    for indivID, data in individuals.items():
        node = Node(data, "individuals")
        masterStore.updateRef("individuals", indivID, node)

def buildDnaGraph(referenceGenome, dnaTestData, masterStore):
    # start building the graph
    for i in range(len(referenceGenome)):
        # store the coordinates of neighboring nodes
        neighbors = set(filter(lambda x: x >= 0.0, 
                               [float(i-1), float(i+1)]))
        # create a new node
        node = Node(referenceGenome[i], "dna", neighbors)

        # store a link to the object in the masterStore
        masterStore.updateRef("dna", float(i), node)

    for indiv in dnaTestData:
        for variant in indiv["dnaData"]:
            # store the coordinates of neighboring nodes
            neighbors = set(filter(lambda x: x >= 0.0, 
                                   [float(int(variant["coordinateStart"] - 1)), 
                                    variant["coordinateStop"]]))

            # create a new node for the individual data
            node = Node(variant["variantAllele"], 
                        "dna",
                        neighbors)

            node.addInterGraphLink("individuals", indiv["individualID"])
            
            if(masterStore.refExists("dna", variant["coordinateStart"])):
                node.merge(masterStore.getRef("dna", variant["coordinateStart"]))

            for neighbor in neighbors:
                if(masterStore.refExists("dna", neighbor)):
                    tempNode = masterStore.getRef("dna", neighbor)
                    tempNode.addNeighbor(variant["coordinateStart"])
                    
            indivNode = masterStore.getRef("individuals", indiv["individualID"])
            indivNode.addInterGraphLink("dna", variant["coordinateStart"])
            
            masterStore.updateRef("dna", variant["coordinateStart"], node)

def dfs(graphName, startNodeID, masterStore):
    q = [startNodeID]
    visited = []

    while(q and masterStore.refExists(graphName, q[0])):
        currentKey = q[0]
        q.remove(currentKey)
        visited.append(currentKey)
        
        node = masterStore.getRef(graphName, currentKey)
        print(str(currentKey) + "\t" + str(node.data) + "\t" + str(node.interGraphLinks))
        
        for i in node.neighbors:
            if i not in visited and i not in q:
                q.append(i)

def moveToRay(masterStore):
    for graph in masterStore.referenceStore:
        for node in masterStore.referenceStore[graph]:
            oid = ray.put(masterStore.referenceStore[graph][node])
            masterStore.referenceStore[graph][node] = oid

if __name__ == "__main__":
    ray.init()
    # all communication to adjacent nodes goes through the master store
    masterStore = MasterStore()

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
    individuals = {0: {"Name":"John Doe", "Gender":"M"}, 1:{"Name":"Jane Doe", "Gender":"M"}}

    buildIndividualsGraph(individuals, masterStore)

    # build the graph
    buildDnaGraph(referenceGenome, dnaTestData, masterStore)

    # traverse our new graph to look at
    dfs("dna", 0.0, masterStore)

    print(masterStore.referenceStore["individuals"][0].interGraphLinks)
    print(masterStore.referenceStore["individuals"][1].interGraphLinks)

    moveToRay(masterStore)

    print(masterStore.referenceStore["individuals"][0])
    print(masterStore.referenceStore["individuals"][1])

    print(ray.get(masterStore.referenceStore["individuals"][0]).interGraphLinks)
    print(ray.get(masterStore.referenceStore["individuals"][1]).interGraphLinks)
