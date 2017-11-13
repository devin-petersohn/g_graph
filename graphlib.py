import ray
import ray.local_scheduler as local_scheduler

# store access to nodes by their global coordinate
@ray.remote
class MasterStore:

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
        
    def get_adjacency_list(self, graph_id, key):
        return ray.get(self.adjacency_list[graph_id].get_adjacency_list.remote(key))

    def bfs(self, graph_id, start_node_id):
        return ray.get(self.adjacency_list[graph_id].bfs.remote(start_node_id))

@ray.remote
class Adjacency_List:
    
    def __init__(self):
        self.oid_dictionary = {}
        self.adjacency_list = {}
        self.inter_graph_connections = {}
        
    def add_adjacency_information(self, key, oid, adjacency_list):
        self.oid_dictionary[key] = oid
        # self.adjacency_list[key] = set(adjacency_list)
        self.adjacency_list[key] = Adj_List_Row(set(adjacency_list))
        self.create_inter_graph_connection(key)
    
    def add_new_adjacent_node(self, key, adjacent_node_key):
        # self.adjacency_list[key].add(adjacent_node_key)
        self.adjacency_list[key].add_connection(adjacent_node_key)
        
    def create_inter_graph_connection(self, key):
        self.inter_graph_connections[key] = {}
        
    def add_inter_graph_connection(self, key, other_graph_id, adjacency_list):
        if not key in self.inter_graph_connections:
            self.create_inter_graph_connection(key)
            self.inter_graph_connections[key][other_graph_id] = set([adjacency_list])
        elif not other_graph_id in self.inter_graph_connections[key]:
            self.inter_graph_connections[key][other_graph_id] = set([adjacency_list])
        else:
            self.inter_graph_connections[key][other_graph_id].add(adjacency_list)
            
    def get_oid_dictionary(self, key = ""):
        if key == "":
            return self.oid_dictionary
        else:
            return self.oid_dictionary[key]
    
    def get_adjacency_list(self, key = ""):
        if key == "":
            return self.adjacency_list
        else:
            return self.adjacency_list[key].get_connections()
            # return self.adjacency_list[key]
        
    def get_inter_graph_connections(self, key = ""):
        if key == "":
            return self.inter_graph_connections
        else:
            return self.inter_graph_connections[key]

    def bfs(self, start_node_id):
        q = [start_node_id]
        visited = []
        nodes_in_bfs_order = []

        while(q and q[0] in self.get_oid_dictionary()):
            current_key = q[0]
            q.remove(current_key)
            visited.append(current_key)

            node = self.get_oid_dictionary(current_key)
            nodes_in_bfs_order.append(node)

            for neighbor in self.get_adjacency_list(current_key):
                if neighbor.destination not in visited and neighbor.destination not in q:
                    q.append(neighbor.destination)

        return nodes_in_bfs_order

class Adj_List_Row:

    def __init__(self, adjacency_list = set()):
        self.adjacency_list = adjacency_list

    def add_connection(self, new_node_key):
        self.adjacency_list.add(new_node_key)

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

def build_individuals_graph(individuals, master_store):
    graph_id = "individuals"
    for indiv_id, data in individuals.items():
        node = Node(indiv_id, data, graph_id)
        master_store.add_node_to_graph.remote(graph_id, indiv_id, node)

@ray.remote
def build_graph_distributed(master_store, graph_id, indiv):
    for variant in indiv["dnaData"]:
        build_node.remote(master_store, variant, graph_id, indiv)

@ray.remote
def build_node(master_store, variant, graph_id, indiv):

    coordinate = variant["coordinateStart"]
    # store the coordinates of neighboring nodes
    neighbors = []
    neighbors.append(Edge(float(int(coordinate) - 1), 0, "left"))
    neighbors.append(Edge(float(int(variant["coordinateStop"])), 0, "right"))

    # create a new node for the individual data
    node = Node(coordinate, variant["variantAllele"], graph_id)
    
    master_store.add_node_to_graph.remote(graph_id, coordinate, node, neighbors)
    master_store.add_inter_graph_connection.remote(graph_id, coordinate, "individuals", indiv["individualID"])
    
    edge_to_this_node = Edge(coordinate, 0, "none")
    for neighbor in neighbors:
        master_store.append_to_adjacency_list.remote(graph_id, neighbor.destination, edge_to_this_node)

def build_dna_graph(reference_genome, dna_test_data, master_store):
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
        master_store.add_node_to_graph.remote(graph_id, coordinate, node, neighbors)
    

    for indiv in dna_test_data:
        # build_graph_distributed.remote(master_store, graph_id, indiv)
        for variant in indiv["dnaData"]:
            
            coordinate = variant["coordinateStart"]
            # store the coordinates of neighboring nodes
            neighbors = []
            neighbors.append(Edge(float(int(coordinate) - 1), 0, "left"))
            neighbors.append(Edge(float(int(variant["coordinateStop"])), 0, "right"))

            # create a new node for the individual data
            node = Node(coordinate, variant["variantAllele"], graph_id)
            
            master_store.add_node_to_graph.remote(graph_id, coordinate, node, neighbors)
            master_store.add_inter_graph_connection.remote(graph_id, coordinate, "individuals", indiv["individualID"])
            
            edge_to_this_node = Edge(coordinate, 0, "none")
            for neighbor in neighbors:
                master_store.append_to_adjacency_list.remote(graph_id, neighbor.destination, edge_to_this_node)

