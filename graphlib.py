import ray
import ray.local_scheduler as local_scheduler

# store access to nodes by their global coordinate
@ray.remote
class Adj_List_Collection:

    def __init__(self):
        self.adjacency_list = {"dna": Adjacency_List.remote(), "rna": Adjacency_List.remote(), "individuals": Adjacency_List.remote()}

    def add_graph(self, graph_id):
        self.adjacency_list[graph_id] = Adjacency_List.remote()
        
    def add_node_to_graph(self, graph_id, key, node, adjacency_list = []):
        oid = node
        if not key in ray.get(self.adjacency_list[graph_id].get_oid_dictionary.remote()):
            if not isinstance(oid, local_scheduler.ObjectID):
                oid = ray.put(node)
            self.add_adjacency_information(graph_id, key, oid, adjacency_list)
        else:
            for new_adjacent_node_key in adjacency_list:
                self.append_to_adjacency_list(graph_id, key, new_adjacent_node_key)
        
    def add_adjacency_information(self, graph_id, key, node_oid, adjacency_list):
        self.adjacency_list[graph_id].add_adjacency_information.remote(key, node_oid, adjacency_list)

    def append_to_adjacency_list(self, graph_id, key, new_adjacent_node_key):
        self.adjacency_list[graph_id].add_new_adjacent_node.remote(key, new_adjacent_node_key)
        
    def add_inter_graph_connection(self, graph_id, key, other_graph_id, other_graph_key):
        self.adjacency_list[graph_id].add_inter_graph_connection.remote(key, other_graph_id, other_graph_key)
        self.adjacency_list[other_graph_id].add_inter_graph_connection.remote(other_graph_key, graph_id, key)
        
    def node_exists(self, graph_id, key):
        return graph_id in self.adjacency_list and ray.get(self.adjacency_list[graph_id].get_adjacency_list.remote(key))
    
    def get_node(self, graph_id, key):
        return ray.get(self.adjacency_list[graph_id].get_oid_dictionary.remote(key))
    
    def get_inter_graph_connections(self, graph_id, key, other_graph_id = ""):
        if other_graph_id == "":
            return ray.get(self.adjacency_list[graph_id].get_inter_graph_connections.remote(key))
        else:
            return ray.get(self.adjacency_list[graph_id].get_inter_graph_connections.remote(key))[other_graph_id]
        
    def get_graph(self, graph_id):
        return self.adjacency_list[graph_id]

    def get_adjacency_list(self, graph_id, key):
        return self.adjacency_list[graph_id].get_adjacency_list.remote(key)

@ray.remote
class Adjacency_List:
    
    def __init__(self):
        self.oid_dictionary = {}
        self.adjacency_list = {}
        self.inter_graph_connections = {}
        
    def add_adjacency_information(self, key, oid, adjacency_list):
        self.oid_dictionary[key] = oid
        #self.adjacency_list[key] = Adj_List_Row(set(adjacency_list))
        if not key in self.adjacency_list:
            self.adjacency_list[key] = ray.put(Adj_List_Row(set(adjacency_list)))
        else:
            self.adjacency_list[key] = add_to_adj_list.remote(self.adjacency_list[key], set(adjacency_list))
        self.create_inter_graph_connection(key)
    
    def add_new_adjacent_node(self, key, adjacent_node_key):
        self.adjacency_list[key] = add_to_adj_list.remote(self.adjacency_list[key], set([adjacent_node_key]))
        
    def create_inter_graph_connection(self, key):
        self.inter_graph_connections[key] = {}
        
    def add_inter_graph_connection(self, key, other_graph_id, adjacency_list):
        if not key in self.inter_graph_connections:
            self.create_inter_graph_connection(key)

        if not other_graph_id in self.inter_graph_connections[key]:
            self.inter_graph_connections[key][other_graph_id] = ray.put(Adj_List_Row(set([adjacency_list])))
        else:
            self.inter_graph_connections[key][other_graph_id] = add_to_inter_graph_connections.remote(self.inter_graph_connections[key][other_graph_id], adjacency_list)
            
    def get_oid_dictionary(self, key = ""):
        if key == "":
            return self.oid_dictionary
        else:
            return self.oid_dictionary[key]
    
    def get_adjacency_list(self, key = ""):
        if key == "":
            return [self.adjacency_list]
        else:
            return [self.adjacency_list[key]]
        
    def get_inter_graph_connections(self, key = ""):
        if key == "":
            return self.inter_graph_connections
        else:
            return self.inter_graph_connections[key]

class Adj_List_Row:

    def __init__(self, adjacency_list = set()):
        self.adjacency_list = adjacency_list

    def add_connection(self, new_node_key):
        self.adjacency_list.add(new_node_key)

    def add_multiple_connections(self, set_of_new_nodes):
        for node in set_of_new_nodes:
            self.add_connection(node)

    def get_connections(self):
        return self.adjacency_list

# a generic node.
class Node:

    def __init__(self, key, data, datatype):
        self.key = key
        self.data = data
        self.datatype = datatype

class Edge:
    
    def __init__(self, destination, weight = 0, orientation = "none"):
        self.destination = destination
        self.weight = weight
        self.orientation = orientation
        
    def update_weight(self, new_weight):
        self.weight = new_weight
        
    def add_to_weight(self, weight_to_add):
        self.weight += weight_to_add
        
    def update_orientation(self, new_orientation):
        self.orientation = new_orientation

@ray.remote
def add_to_adj_list(adj_list, other_key):
    adj_list.add_multiple_connections(other_key)
    return adj_list

@ray.remote
def add_to_inter_graph_connections(inter_graph_connections, new_connection):
    inter_graph_connections.add_connection(new_connection)
    return inter_graph_connections

def build_individuals_graph(individuals, adj_list_collection):
    graph_id = "individuals"
    for indiv_id, data in individuals.items():
        node = Node(indiv_id, data, graph_id)
        adj_list_collection.add_node_to_graph.remote(graph_id, indiv_id, node)

@ray.remote
def build_graph_distributed(adj_list_collection, graph_id, indiv):
    for variant in indiv["dnaData"]:
        build_node.remote(adj_list_collection, variant, graph_id, indiv)

@ray.remote
def build_node(adj_list_collection, variant, graph_id, indiv):

    coordinate = variant["coordinateStart"]
    # store the coordinates of neighboring nodes
    neighbors = []
    neighbors.append(Edge(float(int(coordinate) - 1), 0, "left"))
    neighbors.append(Edge(float(int(variant["coordinateStop"])), 0, "right"))

    # create a new node for the individual data
    node = Node(coordinate, variant["variantAllele"], graph_id)
    
    adj_list_collection.add_node_to_graph.remote(graph_id, coordinate, node, neighbors)
    adj_list_collection.add_inter_graph_connection.remote(graph_id, coordinate, "individuals", indiv["individualID"])
    
    edge_to_this_node = Edge(coordinate, 0, "none")
    for neighbor in neighbors:
        adj_list_collection.append_to_adjacency_list.remote(graph_id, neighbor.destination, edge_to_this_node)

def build_dna_graph(reference_genome, dna_test_data, adj_list_collection):
    graph_id = "dna"
    # start building the graph
    for i in range(len(reference_genome)):
        coordinate = float(i)
        # store the coordinates of neighboring nodes
        neighbors = []

        if i != 0:
            neighbors.append(Edge(float(i - 1), 0, "left"))
        if i != len(reference_genome) - 1:
            neighbors.append(Edge(float(i + 1), 0, "right"))
        
        # create a new node
        node = Node(coordinate, reference_genome[i], graph_id)

        # store a link to the object in the masterStore
        adj_list_collection.add_node_to_graph.remote(graph_id, coordinate, node, neighbors)
    

    for indiv in dna_test_data:
        # build_graph_distributed.remote(adj_list_collection, graph_id, indiv)
        for variant in indiv["dnaData"]:
            
            coordinate = variant["coordinateStart"]
            # store the coordinates of neighboring nodes
            neighbors = []
            neighbors.append(Edge(float(int(coordinate) - 1), 0, "left"))
            neighbors.append(Edge(float(int(variant["coordinateStop"])), 0, "right"))

            # create a new node for the individual data
            node = Node(coordinate, variant["variantAllele"], graph_id)
            
            adj_list_collection.add_node_to_graph.remote(graph_id, coordinate, node, neighbors)
            adj_list_collection.add_inter_graph_connection.remote(graph_id, coordinate, "individuals", indiv["individualID"])
            
            edge_to_this_node = Edge(coordinate, 0, "none")
            for neighbor in neighbors:
                adj_list_collection.append_to_adjacency_list.remote(graph_id, neighbor.destination, edge_to_this_node)

