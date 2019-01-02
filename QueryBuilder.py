from ConfParser import *
import networkx as nx


class Node:
    def __init__(self, name, alias, explode, parent_alias, trans_function):
        self.name = name
        self.alias = alias
        self.explode = explode
        self.parentAlias = parent_alias
        self.trans_function = trans_function

    def __eq__(self, other):
        return self.name == other.name and self.alias == other.alias and self.explode == other.explode \
               and self.parentAlias == other.parentAlias and self.trans_function == other.trans_function

    def __hash__(self):
        return hash(self.name + self.alias + ("True", "False")[self.explode] + self.parentAlias +
                    self.trans_function)


class QueryBuilder:
    @classmethod
    def map_query_empty(cls, n):
        trimmed_name = n.name.replace("[]", "")
        actual_name = (trimmed_name, n.parentAlias + "." + trimmed_name)[not n.parentAlias == ""]
        return "explode(" + actual_name + ") as " + n.alias

    @classmethod
    def map_query_non_empty(cls, map_query, n):
        trimmed_name = n.name.replace("[]", "")
        actual_name = (trimmed_name, n.parentAlias + "." + trimmed_name)[not n.parentAlias == ""]
        return map_query + ",explode(" + actual_name + ") as " + n.alias

    @classmethod
    def map_query_without_exp_empty(cls, n):
        trimmed_name = n.name.replace("[]", "")
        actual_name = (trimmed_name, n.parentAlias + "." + trimmed_name)[not n.parentAlias == ""]
        return actual_name + " as " + n.alias

    @classmethod
    def map_query_without_exp_non_empty(cls, map_query, n):
        trimmed_name = n.name.replace("[]", "")
        actual_name = (trimmed_name, n.parentAlias + "." + trimmed_name)[not n.parentAlias == ""]
        return map_query + "," + actual_name + " as " + n.alias

    @classmethod
    def build_levels(cls, graph, all_table_query, table):
        levels = {}
        always_carry = ""
        size = graph.number_of_nodes()
        for index in range(size):
            cls.reconcile_graph(graph)
            ite = iter(graph)
            removal_list = list()
            layer_exp = False
            map_query = always_carry
            for n in ite:
                    if graph.degree(n) == 0:
                        if n.explode and not layer_exp:
                            if map_query == "":
                                map_query = cls.map_query_empty(n)
                                layer_exp = True
                            else:
                                map_query = cls.map_query_non_empty(map_query, n)
                            removal_list.append(n)
                        elif n.explode and layer_exp:
                            map_query = (n.parentAlias+" as "+n.parentAlias,
                                         map_query + "," + n.parentAlias + " as " + n.parentAlias)[not map_query == ""]
                        else:
                            map_query = (cls.map_query_without_exp_empty(n),
                                         cls.map_query_without_exp_non_empty(map_query, n))[not map_query == ""]
                            always_carry = (n.alias, always_carry + "," + n.alias)[not always_carry == ""]
                            removal_list.append(n)
                    elif graph.in_degree(n) == 0 and not layer_exp:
                        if n.explode:
                            map_query = (cls.map_query_empty(n),
                                         cls.map_query_non_empty(map_query, n))[not map_query == ""]
                            layer_exp = True
                            removal_list.append(n)
                        else:
                            if graph.degree(n) == 0:
                                map_query = (n.name+" as "+n.alias,
                                             map_query + "," + n.name + " as " + n.alias)[not map_query == ""]
                                always_carry = (n.alias, always_carry + "," + n.alias)[not always_carry == ""]
                            map_query = (cls.map_query_without_exp_empty(n),
                                         cls.map_query_without_exp_non_empty(map_query, n))[not map_query == ""]
                            removal_list.append(n)
                    elif graph.in_degree(n) == 0 and layer_exp:
                        if n.explode:
                            if not n.parentAlias == "":
                                if map_query == "":
                                    map_query = n.parentAlias+" as "+n.parentAlias
                                else:
                                    map_query = map_query+","+n.parentAlias+" as "+n.parentAlias
                            else:
                                map_query = (cls.map_query_without_exp_empty(n),
                                             cls.map_query_without_exp_non_empty(map_query, n))[not map_query == ""]
                        else:
                            map_query = (cls.map_query_without_exp_empty(n),
                                         cls.map_query_without_exp_non_empty(map_query, n))[not map_query == ""]
                            removal_list.append(n)
            if not map_query == "":
                levels.__setitem__(index, map_query)
            all_table_query.__setitem__(table.name, levels)
            for n in removal_list:
                if graph.out_degree(n) == 0 and not always_carry.__contains__(n.alias):
                    if always_carry == "":
                        always_carry = n.alias
                    else:
                        always_carry = always_carry + "," + n.alias
                graph.remove_node(n)

    @classmethod
    def reconcile_graph(cls, graph):
        for index in range(graph.number_of_nodes()):
            removal_list = list()
            new_edge_list = {}
            for n in graph.nodes:
                if graph.out_degree(n) > 0:
                    if not n.explode:
                        children = graph.successors(n)
                        do_exit = False
                        child_list = list()
                        for child in children:
                            if child.parentAlias == n.alias:
                                if child.explode:
                                    do_exit = True
                                    break
                                grand_child = graph.successors(child)
                                merge_node = Node(n.name + "." + child.name, child.alias, child.explode, n.parentAlias,
                                                  child.trans_function)
                                child_list.append(merge_node)
                                new_edge_list.__setitem__(merge_node, grand_child)
                                removal_list.append(child)
                        if not do_exit:
                            removal_list.append(n)
                            parents = graph.predecessors(n)
                            for parent in parents:
                                if n.parentAlias == parent.alias:
                                    new_edge_list.__setitem__(parent, child_list)
                        if do_exit:
                            continue
                        break
            for key, value in new_edge_list.items():
                if value:
                    for child in value:
                        if not graph.has_node(key):
                            graph.add_node(key)
                        if not graph.has_node(child):
                            graph.add_node(child)
                        graph.add_edge(key, child)
            for n in removal_list:
                graph.number_of_nodes()
                if graph.has_node(n):
                    graph.remove_node(n)
                    graph.number_of_nodes()

    @classmethod
    def build_graph(cls, table):
        graph = nx.DiGraph()
        graph.size()
        print("parsing columns details into graphical structure, removing the redundancy in column names")
        for col in table.colList:
            source_col_s = col.sourceColumn.split('.')
            parent_node = None
            for index, source_col in enumerate(source_col_s, start=0):
                name = source_col_s[index]
                alias = name
                is_exp = False
                if name.__contains__("[]"):  # this means that the element needs to be exploded in sql query.
                    alias = alias.replace("[]", "")  # alias name should not have explode indicator, removing the same.
                    is_exp = True
                if index == 0:  # this means that this is the top most root element in hierarchy.
                    n = Node(name, alias, is_exp, "", "")  # that is why there is no parent to this node.
                    if not graph.has_node(n):  # this means the node is not the part of graph and a new node.
                        graph.add_node(n)  # Adding the node to graph
                else:  # this means we are traversing intermediate elements
                    n = Node(name, alias, is_exp, parent_node.alias, "")
                    if not graph.has_node(n):
                        if index + 1 == source_col_s.__len__():  # this is last object in path *changes*
                            n.alias = col.targetColumn  # so set up the alias name as well
                        graph.add_node(n)
                        graph.add_edge(parent_node, n)  # adding the edge from parent node to this node
                parent_node = n
        return graph

    @classmethod
    def execute(cls, source, sources, temp_table_name):
        print("building query for tables")
        result = {}
        s = Source(source, None)  # create a source object to compare against the the list of sources
        if s in sources:
            s = sources.__getitem__(sources.index(s))  # Get the actual source object
            target_tables = s.tableList
            if target_tables:
                for table in target_tables:
                    graph = cls.build_graph(table)
                    all_table_query = {}
                    cls.build_levels(graph, all_table_query, table)

                    for key, value in all_table_query.items():
                        inner_map = value
                        # apply the transformation function here
                        function_query_list = list()
                        for col in table.colList:
                            if col.targetProcessing:
                                function_query_list.append(col.targetProcessing)
                            else:
                                function_query_list.append(col.targetColumn)
                        function_query = ", ".join(function_query_list)
                        query_map = {}
                        # to here
                        query = ""
                        for i in range(inner_map.__len__()):
                                if i == 0:
                                    query = "select "+inner_map.get(i)+" from "+temp_table_name
                                else:
                                    if not inner_map.get(i) == inner_map.get(i-1):
                                        query = "select "+inner_map.get(i)+" from ("+query+") map"+i.__str__()
                        query_map.__setitem__("query", query)
                        query_map.__setitem__("function_query", function_query)
                        query_map.__setitem__("audit_query", cls.audit(table))
                        result.__setitem__(key, query_map)
        return result

    @classmethod
    def audit(cls, table):
        audit_list = []
        for col in table.colList:
            if col.audit:
                audits = col.audit.split('|')
                for audit in audits:
                    if audit.__contains__("group"):
                        group_cols = audit.split(" ")[2]
                        group_cols = group_cols.replace("(","").replace(")","")
                        sql = "select '"+table.name+"' as target_table,'{batch_id_placeholder}' as batch_id,'"+col.targetColumn+"' as target_col,'"+audit+"'  as audit_function, '' as audit_value, tab1.audit_value as audit_values from (select collect_list(tab.audit_value) as audit_value from (select concat_ws(':',"+group_cols+", count(*)) as audit_value from {table_placeholder} "+audit+") tab) tab1"
                        audit_list.append(sql)
                    elif audit.__contains__("distinct"):
                        sql = "select '" + table.name + "' as target_table,'{batch_id_placeholder}' as batch_id,'" + col.targetColumn + "' as target_col,'" + audit + "' as audit_function,'' as audit_value, collect_list("+audit+") as audit_values from {table_placeholder} "
                        audit_list.append(sql)
                    else:
                        sql = "select '"+table.name+"' as target_table,'{batch_id_placeholder}' as batch_id,'"+col.targetColumn+"' as target_col,'"+audit+"' as audit_function,"+audit + " as audit_value, collect_list(NULL) as audit_values from {table_placeholder} "
                        audit_list.append(sql)
        return " union all ".join(audit_list)


class QueryMetaData:

    def __init__(self,stage_table, target_table, col_add_change, col_del_change, col_type_change):
        self.stage_table = stage_table
        self.target_table = target_table
        self.col_add_change = col_add_change
        self.col_del_change = col_del_change
        self.col_type_change = col_type_change
