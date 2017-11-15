import graphlib
from graphlib import *
import ray


if __name__ == "__main__":
    ray.init()
    adj_list_collection = Adj_List_Collection.remote()

    reference_genome = "CAGTCCTAGCTACGCTCTATCCTCTCAGAGGACCGATCGATATACGCGTGAAACTAGTGCACTAGACTCGAACTGA"
    dna_test_data = [{"individualID":0, "dnaData":
                    [{"coordinateStart":7.1, "coordinateStop":8.0, "variantAllele": "C"},
                     {"coordinateStart":12.2, "coordinateStop":13.0, "variantAllele": "T"},
                     {"coordinateStart":26.2222, "coordinateStop":27.0, "variantAllele": "TTTT"}]},
                   {"individualID":1, "dnaData":
                    [{"coordinateStart":7.2, "coordinateStop":8.0, "variantAllele": "G"},
                     {"coordinateStart":12.2, "coordinateStop":13.0, "variantAllele": "T"}]}]

    # individual IDs
    individuals = {0: {"Name":"John Doe", "Gender":"M"}, 1: {"Name":"Jane Doe", "Gender":"M"}}

    build_individuals_graph(individuals, adj_list_collection)
    print(ray.get(adj_list_collection.get_node.remote("individuals", 0)).data)
    print(ray.get(adj_list_collection.get_node.remote("individuals", 1)).data)

    # build the graph
    build_dna_graph(reference_genome, dna_test_data, adj_list_collection)

    x = ray.get(adj_list_collection.get_graph.remote("dna"))

    print(x)

    print(ray.get(x.get_oid_dictionary.remote()))

    # traverse our new graph to look at
    # for i in ray.get(ray.get(adj_list_collection.bfs.remote("dna", 0.0))[0]):
    #     i = ray.get(i)
    #     print(str(i.key) + "\t" + str(i.data) + "\t" + str(ray.get(adj_list_collection.get_inter_graph_connections.remote("dna", i.key))))

    print(ray.get(adj_list_collection.get_inter_graph_connections.remote("individuals", 0)))
    print(ray.get(adj_list_collection.get_inter_graph_connections.remote("individuals", 1)))

    print(ray.get(adj_list_collection.get_node.remote("individuals", 0)))
    print(ray.get(adj_list_collection.get_node.remote("individuals", 1)))

    # this will store all reads in their original form
    adj_list_collection.add_graph.remote("reads")
    # this will store the genome graph for all reads
    adj_list_collection.add_graph.remote("reads_genome_graph")

    #sample reads
    sample_read_data = [{"contigName": "chr1", "start": 268051, "end": 268101, "mapq": 0, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:2307:5603:121126", "sequence": "GGAGTGGGGGCAGCTACGTCCTCTCTTGAGCTACAGCAGATTCACTCNCT", "qual": "BCCFDDFFHHHHHJJJIJJJJJJIIIJIGJJJJJJJJJIIJJJJIJJ###", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": False, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "47T0G1", "origQual": None, "attributes": "XT:A:R\tXO:i:0\tXM:i:2\tNM:i:2\tXG:i:0\tXA:Z:chr16,-90215399,50M,2;chr6,-170736451,50M,2;chr8,+71177,50M,3;chr1,+586206,50M,3;chr1,+357434,50M,3;chr5,-181462910,50M,3;chr17,-83229095,50M,3;\tX1:i:5\tX0:i:3", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None},
                        {"contigName": "chr1", "start": 1424219, "end": 1424269, "mapq": 37, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:2107:15569:102571", "sequence": "AGCGCTGTAGGGACACTGCAGGGAGGCCTCTGCTGCCCTGCTAGATGTCA", "qual": "CCCFFFFFHHHHHJJJJJJJJJJIJJJJJJJJJJJJJJJIJJIJHIIGHI", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": False, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "50", "origQual": None, "attributes": "XT:A:U\tXO:i:0\tXM:i:0\tNM:i:0\tXG:i:0\tX1:i:0\tX0:i:1", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None},
                        {"contigName": "chr1", "start": 1443674, "end": 1443724, "mapq": 0, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:2103:19714:5712", "sequence": "TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT", "qual": "############################BBBCDEEA<?:FDCADDD?;=+", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": True, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "50", "origQual": None, "attributes": "XT:A:R\tXO:i:0\tXM:i:0\tNM:i:0\tXG:i:0\tX0:i:1406", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None},
                        {"contigName": "chr1", "start": 1443676, "end": 1443726, "mapq": 0, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:2103:21028:126413", "sequence": "TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT", "qual": "###########################B?;;AFHFIGDDHDDDDDBD@?=", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": True, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "50", "origQual": None, "attributes": "XT:A:R\tXO:i:0\tXM:i:0\tNM:i:0\tXG:i:0\tX0:i:1406", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None},
                        {"contigName": "chr1", "start": 2653642, "end": 2653692, "mapq": 25, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:2306:20003:84408", "sequence": "ANNACACCCCCAGGCGAGCATCTGACAGCCTGGAACAGCACCCACACCCC", "qual": "######JJJJJJJIJIJJIHGGGIIJJJJJJJJJJJJHHFHHDDDBFC@@", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": True, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "0T0C0C47", "origQual": None, "attributes": "XT:A:U\tXO:i:0\tXM:i:3\tNM:i:3\tXG:i:0\tX1:i:0\tX0:i:1", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None},
                        {"contigName": "chr1", "start": 2664732, "end": 2664782, "mapq": 0, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:2106:12935:169714", "sequence": "GAGCATGTGACAGCCTAGGTCGGCACCCACACCCCCAGGTGAGCATCTGA", "qual": "FDBDCHFFEHDCCAFHIHA6EGB?8GGFF?8IEHEB@FHDHGEDDBD@@@", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": True, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "6C9G33", "origQual": None, "attributes": "XT:A:R\tXO:i:0\tXM:i:2\tNM:i:2\tXG:i:0\tX1:i:13\tX0:i:5", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None},
                        {"contigName": "chr1", "start": 2683541, "end": 2683591, "mapq": 0, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:2107:5053:12847", "sequence": "AGCACCCACAACCACAGGTGAGCATCCGACAGCCTGGAACAGCACCCACA", "qual": "CCCFFFFFHGHHHJIJJJHGGIIJJJJJIJGIIJJIJJIJJJJIJIIJJJ", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": False, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "50", "origQual": None, "attributes": "XT:A:R\tXO:i:0\tXM:i:0\tNM:i:0\tXG:i:0\tXA:Z:chr1,+2687435,50M,0;chr1,+2694861,50M,0;chr1,+2755813,50M,1;\tX1:i:1\tX0:i:3", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None},
                        {"contigName": "chr1", "start": 2689861, "end": 2689911, "mapq": 0, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:2108:5080:115408", "sequence": "GGTGAGCATCTGACAGCCCGGAGCAGCACGCAAACCCCCAGGTGAGCATC", "qual": "@@BFBBDFHHHHGJIJGIIFIEIJJJJIJJJJJJJJJJJJIJGHHICEHH", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": False, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "18T3A27", "origQual": None, "attributes": "XT:A:R\tXO:i:0\tXM:i:2\tNM:i:2\tXG:i:0\tX1:i:21\tX0:i:2", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None},
                        {"contigName": "chr1", "start": 2750194, "end": 2750244, "mapq": 0, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:1204:10966:151563", "sequence": "CCCCCNCACCCCCAGGTGAGCATCTGATGGTCTGGAGCAGCACCCACACC", "qual": "######F;JJJJJJJJJJJJIIIJIJJJJFJJIJJGJHHHHHFFDDD?BB", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": True, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "1A3A12C31", "origQual": None, "attributes": "XT:A:R\tXO:i:0\tXM:i:3\tNM:i:3\tXG:i:0\tXA:Z:chr1,-2653118,50M,3;chr1,-2652838,50M,3;chr1,-2653681,50M,3;chr1,-2694823,50M,3;chr1,-2687397,50M,3;chr1,-2755775,50M,3;chr1,-2653921,50M,3;\tX1:i:0\tX0:i:8", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None},
                        {"contigName": "chr1", "start": 3052271, "end": 3052321, "mapq": 25, "readName": "D3NH4HQ1:95:D0MT5ACXX:2:2107:21352:43370", "sequence": "TCANTCATCTTCCATCCATCCGTCCAACAACCATTTGTTGATCATCTCTC", "qual": "@@<#4AD?ACDCDHGIDA>C?<A;8CBEEBAG1D?BG?GH?@DEHFG@FH", "cigar": "50M", "readPaired": False, "properPair": False, "readMapped": True, "mateMapped": False, "failedVendorQualityChecks": False, "duplicateRead": False, "readNegativeStrand": False, "mateNegativeStrand": False, "primaryAlignment": True, "secondaryAlignment": False, "supplementaryAlignment": False, "mismatchingPositions": "3C44A0T0", "origQual": None, "attributes": "XT:A:U\tXO:i:0\tXM:i:3\tNM:i:3\tXG:i:0\tX1:i:0\tX0:i:1", "recordGroupName": None, "recordGroupSample": None, "mateAlignmentStart": None, "mateAlignmentEnd": None, "mateContigName": None, "inferredInsertSize": None}]

    for read in sample_read_data:
        adj_list_collection.add_node_to_graph.remote("reads", read["readName"], Node(read["readName"], read, "reads"))
        for index in range(len(read["sequence"])):
            data = read["sequence"][index]

            neighbors = []
            if index != 0:
                neighbors.append(Edge(read["contigName"] + "\t" + str(read["start"] + index - 1), 0, "left"))
            if index != len(read["sequence"]) - 1:
                neighbors.append(Edge(read["contigName"] + "\t" + str(read["start"] + index + 1), 0, "right"))
                
            coordinate = read["contigName"] + "\t" + str(read["start"] + index)
            node = Node(coordinate, data, "reads_genome_graph")

            adj_list_collection.add_node_to_graph.remote("reads_genome_graph", coordinate, node, neighbors)
            adj_list_collection.add_inter_graph_connection.remote("reads_genome_graph", coordinate, "reads", read["readName"])

    # for storing the feature data
    adj_list_collection.add_graph.remote("features")

    sampleFeatures = [{"featureName": "0", "contigName": "chr1", "start": 45520936, "end": 45522463, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "878", "thickStart": "482.182760214932", "thickEnd": "-1"}},
                        {"featureName": "1", "contigName": "chr1", "start": 88891087, "end": 88891875, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "423", "thickStart": "446.01797654123", "thickEnd": "-1"}},
                        {"featureName": "2", "contigName": "chr1", "start": 181088138, "end": 181090451, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "626", "thickStart": "444.771802710521", "thickEnd": "-1"}},
                        {"featureName": "3", "contigName": "chr1", "start": 179954184, "end": 179955452, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "647", "thickStart": "440.10466093652", "thickEnd": "-1"}},
                        {"featureName": "4", "contigName": "chr1", "start": 246931401, "end": 246932507, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "423", "thickStart": "436.374938660247", "thickEnd": "-1"}},
                        {"featureName": "5", "contigName": "chr1", "start": 28580676, "end": 28582443, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "1106", "thickStart": "434.111845970505", "thickEnd": "-1"}},
                        {"featureName": "6", "contigName": "chr1", "start": 23691459, "end": 23692369, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "421", "thickStart": "426.055504846001", "thickEnd": "-1"}},
                        {"featureName": "7", "contigName": "chr1", "start": 201955033, "end": 201956082, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "522", "thickStart": "423.882565088207", "thickEnd": "-1"}},
                        {"featureName": "8", "contigName": "chr1", "start": 207321011, "end": 207323021, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "741", "thickStart": "423.625988483304", "thickEnd": "-1"}},
                        {"featureName": "9", "contigName": "chr1", "start": 145520936, "end": 145522463, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "878", "thickStart": "482.182760214932", "thickEnd": "-1"}},
                        {"featureName": "10", "contigName": "chr1", "start": 188891087, "end": 188891875, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "423", "thickStart": "446.01797654123", "thickEnd": "-1"}},
                        {"featureName": "11", "contigName": "chr1", "start": 1181088138, "end": 1181090451, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "626", "thickStart": "444.771802710521", "thickEnd": "-1"}},
                        {"featureName": "12", "contigName": "chr1", "start": 1179954184, "end": 1179955452, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "647", "thickStart": "440.10466093652", "thickEnd": "-1"}},
                        {"featureName": "13", "contigName": "chr1", "start": 1246931401, "end": 1246932507, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "423", "thickStart": "436.374938660247", "thickEnd": "-1"}},
                        {"featureName": "14", "contigName": "chr1", "start": 128580676, "end": 128582443, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "1106", "thickStart": "434.111845970505", "thickEnd": "-1"}},
                        {"featureName": "15", "contigName": "chr1", "start": 123691459, "end": 123692369, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "421", "thickStart": "426.055504846001", "thickEnd": "-1"}},
                        {"featureName": "16", "contigName": "chr1", "start": 1201955033, "end": 1201956082, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "522", "thickStart": "423.882565088207", "thickEnd": "-1"}},
                        {"featureName": "17", "contigName": "chr1", "start": 1207321011, "end": 1207323021, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "741", "thickStart": "423.625988483304", "thickEnd": "-1"}},
                        {"featureName": "18", "contigName": "chr1", "start": 1110963118, "end": 1110964762, "score": 0.0, "attributes": {"itemRgb": "5.0696939910406", "blockCount": "758", "thickStart": "421.056761458099", "thickEnd": "-1"}}]
    try:
        for feature in sampleFeatures:
            node = Node(feature["featureName"], feature, "features")
            adj_list_collection.add_node_to_graph.remote("features", feature["featureName"], node)
            for index in range(feature["end"] - feature["start"]):
                coordinate = feature["contigName"] + "\t" + str(read["start"] + index)
                adj_list_collection.add_inter_graph_connection.remote("features", feature["featureName"], "reads_genome_graph", coordinate)

        # print("BFS BEGIN")

        # for i in ray.get(adj_list_collection.bfs.remote("reads_genome_graph", "chr1\t1443674")):
        #     print(str(i.key) + "\t" + str(i.data))
    except Exception as e:
        print("Something happened: " + str(e))
