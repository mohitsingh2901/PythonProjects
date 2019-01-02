import abc


class ReaderPlugin(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def execute(self, session, queries, file_path, temp_table_name, batch_id, row_tag, work_dir):
        pass
