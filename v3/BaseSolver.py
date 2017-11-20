from pulp import *
import time
import pandas as pd
import boto3



class BaseSolver (object):
    def __init__(self,
                 input_path,
                 output_path,
                 bucket,
                 logger,
                 objective_type,
                 description,
                 hour=time.gmtime().tm_hour):

        self.input_path = input_path
        self.output_path = output_path
        self.logger = logger
        self.hour = hour
        self.s3 = boto3.client('s3')
        self.bucket=bucket
        self.decision_variables = {}


        if objective_type == 'max':
            objective_type = LpMaximize
        else:
            objective_type = LpMinimize
        self.solver_model = LpProblem(description,objective_type)



    def get_input(self,is_spark = True):
        if is_spark == True:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.builder.appName('solver').getOrCreate()
            self.df = self.spark.read.csv(self.input_path +'/*',sep='\t').toPandas()
        else:
            files = self.s3.list_objects(Bucket=self.bucket, Prefix=self.input_path)
            a = [pd.read_csv("s3://{}/{}".format(self.bucket, i['Key']), sep='\t') for i in files['Contents'] if i['Size'] > 0]
            self.df = pd.concat(a)
            self.df['index'] = self.df.index

    def preprocessing(self,**kwargs):
        pass

    def create_variables(self,**kwargs):
        pass

    def create_soft_constraint(self,
                               variables,
                               inequality,
                               name,
                               sense,
                               penalty,
                               proportionFreeBound,
                               affine=None, **kwargs):
        if not affine:
            lpvars = [self.decision_variables[i] for i in variables]
            c = lpSum(lpvars)
        else:
            c = LpAffineExpression([(self.decision_variables[i], j) for i,j in variables])
        self.logger.debug("zone_id: {}".format(str(name)))
        constraint = LpConstraint(e=c, sense=sense, name= str(name), rhs=inequality)
        self.solver_model.extend(
            constraint.makeElasticSubProblem(penalty=penalty, proportionFreeBound=proportionFreeBound))


    def create_constraint(self,args):
        decision_vars_dict = args[0]
        target = args[1]
        values = args[2]
        soft_constraint_dict = args[3]
        vars = decision_vars_dict[values]
        inequality = target[values]

        if not soft_constraint_dict:
            lpvars = [self.decision_variables[i] for i in vars]
            self.solver_model += lpSum(lpvars) <= inequality
        else:
            name_prefix = soft_constraint_dict['name_prefix']
            sense = soft_constraint_dict['sense']
            penalty= soft_constraint_dict['penalty']
            proportionFreeBound = soft_constraint_dict['proportionFreeBound']

            self.create_soft_constraint(variables= vars,
                                        inequality= inequality/5,
                                        name=name_prefix+ values,
                                        sense=sense,
                                        penalty=penalty,
                                        proportionFreeBound=proportionFreeBound
                                        )
        return True


    def add_constraint(self,**kwargs):

        pass


    def run_solver(self):
        pass


    def generate_output(self):
        pass






