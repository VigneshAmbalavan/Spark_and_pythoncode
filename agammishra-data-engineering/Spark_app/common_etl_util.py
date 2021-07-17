import logging


class JobConfig():
    def __init__(self, config, additional_param=None):
        default_parameter_list = (
            'input_file_path', 'source_file_type', 'write_format', 'write_mode', 'output_file_path', 'partition_col',
            'repartition')
        for param in default_parameter_list:
            setattr(self, param, JobConfig.get_param_value(config, param))
        if additional_param is not None:
            for param in additional_param:
                setattr(self, param, JobConfig.get_param_value(config, param))

    @staticmethod
    def get_param_value(config, param, default=None):
        param = config.get('SectionETL', param)
        return param


class CommonEtl(JobConfig):
    def __init__(self, spark, config: JobConfig):
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger('py4j')

    def pipeline_list(self):
        return [self.extract_data, self.transform, self.load]

    def extract_data(self, df):
        try:
            source_df = self.spark.read.format(self.config.source_file_type).load(self.config.input_file_path)
            return source_df
        except Exception as e:
            self.logger.error('failed in extracting the data')
            raise e

    def transform(self, df):
        pass

    def load(self, df):
        try:
            if self.config.partition_col:
                df.repartition(int(self.config.repartition))\
                    .write.format(self.config.write_format)\
                    .mode(self.config.write_mode).option('header',True)\
                    .partitionBy(self.config.partition_col.split(',')[0],self.config.partition_col.split(',')[1])\
                    .save(self.config.output_file_path)
            else:
                df.repartition(int(self.config.repartition)).write.format(self.config.write_format)\
                    .mode(self.config.write_mode).option('header', True)\
                    .save(self.config.output_file_path)
        except Exception as e:
            self.logger.error("following  exception occurred while loading the data", e)
            raise e

    def execute(self):
        df = None
        try:
            for func in self.pipeline_list():
                df = func(df)
        except Exception as e:
            self.logger.error("following error came while running {}".format(func), e)
            raise e
