import csv


class ConfigParser:

    @staticmethod
    def conf_execute(conf_path):
        source_list = list()
        with open(conf_path, 'r') as f:
            reader = csv.reader(f, delimiter='~')
            for row in reader:
                s = Source(row[0], None)
                t = TargetTables(row[1], None)
                col = Column(row[2], row[3], row[4], row[5], row[6])
                if s in source_list:
                    s = source_list.__getitem__(source_list.index(s))
                    if t in s.tableList:
                        t = s.tableList.__getitem__(s.tableList.index(t))
                        if col not in t.colList:
                            t.colList.append(col)
                    else:
                        col_list = list()
                        col_list.append(col)
                        t.colList = col_list
                        s.tableList.append(t)
                else:
                    col_list = list()
                    col_list.append(col)
                    t.colList = col_list
                    table_list = list()
                    table_list.append(t)
                    s.tableList = table_list
                    source_list.append(s)
            return source_list


class Source:
    def __init__(self, source, table_list):
        self.name = source
        self.tableList = table_list

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)


class TargetTables:
    def __init__(self, name, col_list):
        self.name = name
        self.colList = col_list

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)


class Column:

    def __init__(self, base_path, source_column, target_column, audit, target_processing):
        if base_path != "":
            self.sourceColumn = base_path + "." + source_column
        else:
            self.sourceColumn = source_column
        self.basePath = base_path
        self.targetColumn = target_column
        self.audit = audit
        self.targetProcessing = target_processing

    def __eq__(self, other):
        return self.basePath == other.basePath and self.sourceColumn == other.sourceColumn and \
            self.targetColumn == other.targetColumn and self.audit == other.audit and \
            self.targetProcessing == other.targetProcessing

    def __hash__(self):
        return hash(self.sourceColumn + self.basePath + self.targetColumn + self.targetProcessing + self.audit)
