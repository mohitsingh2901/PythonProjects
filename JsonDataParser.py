from FileFormatReader import ReaderPlugin
from QueryBuilder import QueryMetaData
import sys


class JsonDataParser(ReaderPlugin):

    def execute(self, session, queries, query, temp_table_name, batch_id, row_tag, work_dir):
        #file_path = work_dir + "/" + temp_table_name + "_" + batch_id
        #session.sql(query).write.format("text").save(file_path)
        m_list = list()
        data_frame = session.read.option("multiLine", True).json(query)
        data_frame.createTempView(temp_table_name)
        data_frame.printSchema()
        session.sql("cache table " + temp_table_name)

        for key, outer_value in queries.items():
                value = outer_value.get("query")
                print("Sql query for table ***** " + key + " *****" + value)
                session.sql("SET spark.sql.orc.enabled=true")
                try:
                    if key.__contains__("."):
                        temp_name = key.split('.')[1]
                    else:
                        temp_name = key
                    session.sql(value).createTempView(temp_name)
                except BaseException:
                    print("Unexpected error:", sys.exc_info()[0])
                    raise

                #session.sql("CREATE TABLE " + key + "_" + batch_id + " STORED AS ORC AS SELECT "
                #                       + outer_value.get("function_query") + " from " + temp_name)
                session.sql("SELECT " + outer_value.get("function_query") + " from " + temp_name).show()

                audit_query = outer_value.get("audit_query").replace("{batch_id_placeholder}", batch_id)\
                    .replace("{table_placeholder}", temp_name)
                session.sql(audit_query).show()
                m = QueryMetaData
                m.stage_table = key + "_" + batch_id
                m.target_table = key
                m_list.append(m)
        return m_list