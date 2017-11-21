import ray
import ray.local_scheduler as local_scheduler
import warnings

class Graph_collection:
    """
    This object manages all graphs in the system.

    Fields:
    graph_dict -- the dictionary of adjacency lists for the graphs.
                           The keys for the dictionary are the graph_ids, and
                           the values are the Graph objects.
    """
    def __init__(self):
        """
        The constructor for an Graph_collection object. Initializes some toy
        graphs as an example.
        """
        self.graph_dict = {}

    def add_graph(self, graph_id):
        """
        Create an empty graph.

        Keyword arguments:
        graph_id -- the name of the new graph.
        """
        if not graph_id or graph_id == "":
            raise ValueError("Graph must be named something.")
        if graph_id in self.graph_dict:
            raise ValueError("Graph name already exists.")
        self.graph_dict[graph_id] = Graph.remote()
        
    def add_node_to_graph(self, graph_id, key, node, adjacency_list = set(), connections_to_other_graphs = {}):
        """
        Adds data to the graph specified.

        Keyword arguments:
        graph_id -- the unique name of the graph.
        key -- the unique identifier of this data in the graph.
        node -- the data to add to the graph.
        adjacency_list -- a list of connected nodes, if any (default = set()).
        """
        if type(connections_to_other_graphs) is not dict:
            raise ValueError("Connections between graphs must be labeled with a destination graph.")

        if graph_id not in self.graph_dict:
            warnings.warn(str(graph_id) + " is not yet in this Graph Collection. Creating...", Warning)
            self.add_graph(graph_id)

        _add_node_to_graph.remote(self.graph_dict[graph_id], graph_id, key, node, adjacency_list, connections_to_other_graphs)
        _add_back_edges_within_graph.remote(self.graph_dict[graph_id], graph_id, key, adjacency_list)

        for new_conn in connections_to_other_graphs:
            if not new_conn in self.graph_dict:
                warnings.warn(str(new_conn) + " is not yet in this Graph Collection. Creating...", Warning)
                self.add_graph(new_conn)

            try:
                connections_to_this_graph = set(connections_to_other_graphs[new_conn])
            except TypeError:
                connections_to_this_graph = set([connections_to_other_graphs[new_conn]])

            _add_back_edges_between_graphs.remote(self.graph_dict[new_conn], key, graph_id, connections_to_this_graph)

    def append_to_connections(self, graph_id, key, new_adjacent_node_key):
        """
        Adds a new connection to the graph for the key provided.

        Keyword arguments:
        graph_id -- the unique name of the graph.
        key -- the unique identifier of the node in the graph.
        new_adjacent_node_key -- the unique identifier of the new connection.
        """
        self.graph_dict[graph_id].add_new_adjacent_node.remote(key, new_adjacent_node_key)
        
    def add_inter_graph_connection(self, graph_id, key, other_graph_id, other_graph_key):
        """
        Adds a new connection to another graph. Because all connections
        are bi-directed, connections are created from the other graph to this
        one also.

        Keyword arguments:
        graph_id -- the unique name of the graph.
        key -- the unique identifier of the node in the graph.
        other_graph_id -- the unique name of the graph to connect to.
        other_graph_key -- the unique identifier of the node to connect to.
        """
        self.graph_dict[graph_id].add_inter_graph_connection.remote(key, other_graph_id, other_graph_key)
        self.graph_dict[other_graph_id].add_inter_graph_connection.remote(other_graph_key, graph_id, key)

    def add_multiple_inter_graph_connections(self, graph_id, key, other_graph_id, collection_of_other_graph_keys):
        """
        Adds multiple new connections to another graph.

        Keyword arguments:
        graph_id -- the unique name of the graph.
        key -- the unique identifier of the node in the graph.
        other_graph_id -- the unique name of the graph to connect to.
        collection_of_other_graph_keys -- the collection of unique identifier
                                          of the node to connect to.
        """
        self.graph_dict[graph_id].add_multiple_inter_graph_connections.remote(key, other_graph_id, collection_of_other_graph_keys)
        _add_back_edges_between_graphs.remote(self.graph_dict[other_graph_id], key, graph_id, collection_of_other_graph_keys)
        
    def node_exists(self, graph_id, key):
        """
        Determines whether or not a node exists in the graph.

        Keyword arguments:
        graph_id -- the unique name of the graph
        key -- the unique identifier of the node in the graph.

        Returns:
        True if both the graph exists and the node exists in the graph,
        false otherwise
        """
        return graph_id in self.graph_dict and self.graph_dict[graph_id].node_exists.remote(key)
    
    def get_node(self, graph_id, key):
        """
        Gets the ObjectID for a node in the graph requested.

        Keyword arguments:
        graph_id -- the unique name of the graph.
        key -- the unique identifier of the node in the graph.

        Returns:
        The Ray ObjectID from the graph and key combination requested.
        """
        return self.graph_dict[graph_id].get_oid_dictionary.remote(key)
    
    def get_inter_graph_connections(self, graph_id, key, other_graph_id = ""):
        """
        Gets the connections between graphs for the node requested. Users can
        optionally specify the other graph they are interested in.

        Keyword arguments:
        graph_id -- the unique name of the graph.
        key -- the unique identifier of the node in the graph.
        other_graph_id -- the name of the other graph (default = "")

        Returns:
        When other_graph_id is "", all connections between graphs for
        the graph and key requested. Otherwise, the connections for the
        graph specified in other_graph_id for the graph and key requested.
        """
        if other_graph_id == "":
            return self.graph_dict[graph_id].get_inter_graph_connections.remote(key)
        else:
            return self.graph_dict[graph_id].get_inter_graph_connections.remote(key, other_graph_id)
        
    def get_graph(self, graph_id):
        """
        Gets the graph requested.

        Keyword arguments:
        graph_id -- the unique name of the graph.

        Returns:
        The Graph object for the graph requested.
        """
        return self.graph_dict[graph_id]

    def get_adjacency_list(self, graph_id, key):
        """
        Gets the adjacency list for the graph and key requested.

        Keyword arguments:
        graph_id -- the unique name of the graph.
        key -- the unique identifier of the node in the graph.

        Returns:
        The list of all connections within the same graph for the node
        requested.
        """
        return self.graph_dict[graph_id].get_adjacency_list.remote(key)

@ray.remote
class Graph:
    """
    This object contains reference and connection information for a graph.

    Fields:
    oid_dictionary -- the dictionary mapping the unique identifier of a node to
                      the ObjectID of the Node object stored in the Ray object
                      store.
    adjacency_list -- the dictionary mapping the unique identifier of a node to
                      an ObjectID of the set of connections within the graph.
                      The set of connections is built asynchronously.
    inter_graph_connections -- the dictionary mapping the unique identifier of
                               a node to the ObjectID of the set of connections
                               between graphs. The set of connections between
                               graphs is a dictionary {graph_id -> other_graph_key}
                               and is built asynchronously.
    """
    
    def __init__(self):
        """
        The constructor for the Graph object. Initializes all graph data.
        """
        self.oid_dictionary = {}
        self.adjacency_list = {}
        self.inter_graph_connections = {}
        
    def insert_node_into_graph(self, key, oid, adjacency_list, connections_to_other_graphs):
        """
        Inserts the data for a node into the graph.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.
        oid -- the Ray ObjectID for the Node object referenced by key.
        adjacency_list -- the list of connections within this graph.
        """
        if type(connections_to_other_graphs) is not dict:
            raise ValueError("Connections to other graphs require destination graph to be specified.")

        self.oid_dictionary[key] = ray.put(oid)
        if not key in self.adjacency_list:
            self.adjacency_list[key] = ray.put(set(adjacency_list))
        else:
            self.adjacency_list[key] = _add_to_adj_list.remote(self.adjacency_list[key], set(adjacency_list))

        if not key in self.inter_graph_connections:
            self.inter_graph_connections[key] = {}
        
        for other_graph_id in connections_to_other_graphs:
            if not other_graph_id in self.inter_graph_connections[key]:
                try:
                    if type(connections_to_other_graphs) is str:
                        self.inter_graph_connections[key][other_graph_id] = ray.put(set([connections_to_other_graphs[other_graph_id]]))
                    self.inter_graph_connections[key][other_graph_id] = ray.put(set(connections_to_other_graphs[other_graph_id]))
                except TypeError:
                    self.inter_graph_connections[key][other_graph_id] = ray.put(set([connections_to_other_graphs[other_graph_id]]))
            else:
                self.inter_graph_connections[key][other_graph_id] = _add_to_adj_list.remote(self.inter_graph_connections[key][other_graph_id], connections_to_other_graphs[other_graph_id])
    
    def add_new_adjacent_node(self, key, adjacent_node_key):
        """
        Adds a new connection to the adjacency_list for the key provided.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.
        adjacent_node_key -- the unique identifier of the new connection to be
                             added.
        """
        if not key in self.adjacency_list:
            self.adjacency_list[key] = set([adjacent_node_key])
        else:
            self.adjacency_list[key] = _add_to_adj_list.remote(self.adjacency_list[key], adjacent_node_key)
        
    def create_inter_graph_connection(self, key):
        """
        Initializes the inter_graph_connections for a given identifier.
        """
        self.inter_graph_connections[key] = {}
        
    def add_inter_graph_connection(self, key, other_graph_id, new_connection):
        """
        Adds a single new connection to another graph. Because all connections
        are bi-directed, connections are created from the other graph to this
        one also.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.
        other_graph_id -- the name of the graph for the new connection.
        new_connection -- the identifier of the node for the new connection.
        """
        if not key in self.inter_graph_connections:
            self.create_inter_graph_connection(key)

        if not other_graph_id in self.inter_graph_connections[key]:
            self.inter_graph_connections[key][other_graph_id] = ray.put(set([new_connection]))
        else:
            self.inter_graph_connections[key][other_graph_id] = _add_to_adj_list.remote(self.inter_graph_connections[key][other_graph_id], set([new_connection]))

    def add_multiple_inter_graph_connections(self, key, other_graph_id, new_connection_list):
        """
        Adds a multiple new connections to another graph. Because all
        connections are bi-directed, connections are created from the other
        graph to this one also.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.
        other_graph_id -- the name of the graph for the new connection.
        new_connection_list -- the list of identifiers of the node for the new
                               connection.
        """
        if not key in self.inter_graph_connections:
            self.create_inter_graph_connection(key)

        if not other_graph_id in self.inter_graph_connections[key]:
            self.inter_graph_connections[key][other_graph_id] = ray.put(set(new_connection_list))
        else:
            self.inter_graph_connections[key][other_graph_id] = _add_to_adj_list.remote(self.inter_graph_connections[key][other_graph_id], set(new_connection_list))

    def node_exists(self, key):
        """
        Determines if a node exists in the graph.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.

        Returns:
        If node exists in graph, returns true, otherwise false.
        """
        return key in self.oid_dictionary

    def get_oid_dictionary(self, key = ""):
        """
        Gets the ObjectID of the Node requested. If none requested, returns the
        full dictionary.

        Keyword arguments:
        key -- the unique identifier of the node in the graph (default = "").
        """
        if key == "":
            return self.oid_dictionary
        else:
            return self.oid_dictionary[key]
    
    def get_adjacency_list(self, key = ""):
        """
        Gets the connections within this graph of the Node requested. If none
        requested, returns the full dictionary.

        Keyword arguments:
        key -- the unique identifier of the node in the graph (default = "").
        """
        if key == "":
            return self.adjacency_list
        else:
            return self.adjacency_list[key]
        
    def get_inter_graph_connections(self, key = "", other_graph_id = ""):
        """
        Gets the connections to other graphs of the Node requested. If none
        requested, returns the full dictionary.

        Keyword arguments:
        key -- the unique identifier of the node in the graph (default = "").
        """
        if key == "":
            return self.inter_graph_connections
        elif other_graph_id == "":
            return self.inter_graph_connections[key]
        else:
            raise ValueError("Not yet implemented: Getting inter-graph connections for a specific graph")
            return self.inter_graph_connections[key][other_graph_id]

class Node:
    """
    This object is a generic node, the basic component of a Graph.

    Fields:
    data -- the data this node will contain. This data can be any format.
    """
    def __init__(self, data):
        self.data = data

class Edge:
    """
    This object is an edge, or connection, between Nodes in a Graph.

    Fields:
    destination -- the destination key of the connection.
    weight -- a value to represent the strength of the connection. If the
              connection is unweighted, choose weight = 0 (the default).
    orientation -- the direction of the connection. If orientation is
                   irrelevent, choose orientation = none (the default).
    """
    def __init__(self, destination, weight = 0, orientation = "none"):
        """
        The constructor for an Edge object.

        Keyword arguments:
        destination -- the destination key of the connection.
        weight -- a value to represent the strength of the connection. If the
                  connection is unweighted, choose weight = 0 (the default).
        orientation -- the direction of the connection. If orientation is
                       irrelevent, choose orientation = none (the default).
        """
        self.destination = destination
        self.weight = weight
        self.orientation = orientation
        
    def update_weight(self, new_weight):
        """
        Updates the weight in this Edge to the value provided.

        Keyword arguments:
        new_weight -- the new weight for this Edge.
        """
        self.weight = new_weight
        
    def add_to_weight(self, weight_to_add):
        """
        Adds a value to the existing weight and updates it.

        Keyword arguments:
        weight_to_add -- the weight to add to the weight for this Edge.
        """
        self.weight += weight_to_add
        
    def update_orientation(self, new_orientation):
        """
        Updates the orientation of this Edge to the value provided.

        Keyword arguments:
        new_orientation -- the value to replace the orientation in this Edge.
        """
        self.orientation = new_orientation

@ray.remote
def _add_node_to_graph(graph, graph_id, key, node, adjacency_list, connections_to_other_graphs):
    """
    Adds a node to the graph provided and associates it with the connections.

    Keyword arguments:
    graph -- the Graph object to add the node to.
    graph_id -- the unique identifier of the Graph provided.
    key -- the unique identifier of the node provided.
    node -- the Node object to add to the graph.
    adjacency_list -- the list of connections within this graph.
    """
    graph.insert_node_into_graph.remote(key, node, adjacency_list, connections_to_other_graphs)
    # if not ray.get(graph.node_exists.remote(key)):
    #     graph.insert_node_into_graph.remote(key, node, adjacency_list, connections_to_other_graphs)
    # else:
    #     #TODO: Figure out how to handle this best.
    #     # raise ValueError("Key: " + str(key) + " already exists in graph: " + graph_id + ".")
    #     graph.add_new_adjacent_node.remote(key, adjacency_list)
    #     for other_graph_id in connections_to_other_graphs:
    #         graph.add_inter_graph_connection.remote(graph.get_inter_graph_connections, other_graph_id, connections_to_other_graphs[other_graph_id])
        

@ray.remote
def _add_to_adj_list(adj_list, other_key):
    """
    Adds one or multiple keys to the list provided. This can add to both the
    adjacency list and the connections between graphs.

    The need for this stems from trying to make updates to the graph as
    asynchronous as possible.

    Keyword arguments:
    adj_list -- the list of connections to append to.
    other_key -- a set of connections or a single value to add to adj_list.

    Returns:
    The updated list containing the newly added value(s).
    """
    try:
        adj_list.update(set(other_key))
    except TypeError:
        adj_list.add(other_key)
    
    return adj_list    

@ray.remote
def _add_back_edges_within_graph(graph, graph_id, key, new_connection_list):
    """
    Adds back edges to the connections provided. This achieves the
    bi-drectionality guarantees we have.

    Keyword arguments:
    graph -- the Graph object to add the back edges to.
    graph_id -- the unique identifier of the graph provided.
    key -- the unique identifier of the Node to connect back edges to.
    new_connection_list -- the list of connections to create back edges for.
    """
    for new_conn in new_connection_list:
        graph.add_new_adjacent_node.remote(key, new_conn)

@ray.remote
def _add_back_edges_between_graphs(other_graph, key, graph_id, collection_of_other_graph_keys):
    """
    Given a list of keys in another graph, creates connections to the key
    provided. This is used to achieve the bi-drectionality in the graph.

    Keyword arguments:
    other_graph -- the Graph object of the other graph for the connections to
                   be added.
    key -- the key to connect the other graph keys to.
    graph_id -- the unique identifier of the graph to connect to.
    collection_of_other_graph_keys -- the keys in other_graph to connect to key.
    """
    for other_graph_key in collection_of_other_graph_keys:
        other_graph.add_inter_graph_connection.remote(other_graph_key, graph_id, key)
