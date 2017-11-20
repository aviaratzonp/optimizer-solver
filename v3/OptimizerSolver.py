import BaseSolver
import numpy as np
import pulp
from pulp import *



class OptimizerSolver(BaseSolver):

    def preprocessing(self,**kwargs):

        hours = kwargs['hour']

        # step #1- remove all unrelevant slices
        #step1.1 - all campaigns with ndq=0 and skip_daily
        self.logger.info("total number of rows before filtering: {}".format(self.df.shape[0]))
        self.df = self.df.loc[~(self.df.needed_daily_quota == 0) & (self.df.skip_daily_goal == False)]
        self.logger.info("total number of rows after filtering NDQ=0 and skip daily goal= False: {}".format(self.df.shape[0]))

        # step1.2 - remove all slices with forecast!=0
        self.df = self.df.loc[self.df['forecast'] != 0]
        self.logger.info("total number of rows after filtering forecast 0: {}".format(self.df.shape[0]))



        #step #2- convert metrics to campaign level
        self.convert_metrics_to_campaign_level()

        #step #3- concat features
        self.df['features'] = self.df.country.astype(str).str.cat(self.df.region.astype(str), sep='|')

        # step #4 - calculate remaining
        self.df.info("calculate remaining impressions...")
        self.df['remaining'] = np.floor(np.maximum(0,
                                                  np.where(self.df['skip_daily_goal'] == True,
                                                           self.df['units'] - self.df['lifetime_impressions'],
                                                           self.df['needed_daily_quota'] - self.df['today_impressions'])))

        # step #5- convert 0 KPIgoal to pacing
        self.df.loc[(self.df['kpi_goal'] == 0), 'kpi_type'] = 'PACING'

        # step #6- split default slices from dataframe
        self.default_df = self.df.loc[(self.df['zone_id'] < 0) | (self.df['is_programmatic'] == True)]
        self.logger.info("total number of default slices: {}".format(self.default_df.shape[0]))

        # step7 - remove all slices with hour < current hour
        self.df = self.df.loc[self.df['hour'] >= hours]
        self.logger.info("total number of rows after filtering past hours: {}".format(self.df.shape[0]))
        self.df = self.df[(self.df['zone_id'] > 0) & (self.df['is_programmatic'] == False)]


        # step #2- split the default slices
        self.handle_default_slices(**kwargs)
        # step #3- split programmatic
        self.handle_programmatic_slices()

    def handle_default_slices(self,**kwargs):
        default_portion = kwargs['default_pct']

        #find # of banners under campaign
        campaign_divisor_dict = self.default_df.groupby('campaign_id')['banner_id'].nunique().to_frame().banner_id.to_dict()

        self.default_df['buy_at_most'] = self.default_df.apply(lambda x: int(int(x['remaining'] * default_portion) / (campaign_divisor_dict[x['campaign_id']])),axis=1)

        self.logger.info("amount of is programmatic slices: {}".format(str(self.default_df.loc[self.default_df['is_programmatic'] == True].shape)))

        self.default_df.loc[(self.default_df['is_programmatic'] == True), 'buy_at_most'] = -1
        self.default_df.loc[(self.default_df['is_programmatic'] == True), 'zone_id'] = -2


    def convert_metrics_to_campaign_level(self):
        campaign_level_metrics = self.df[
            ['campaign_id', 'banner_id', 'lifetime_impressions', 'lifetime_metric_numerator', 'today_impressions',
             'today_metric_numerator']].drop_duplicates() \
            .groupby('campaign_id')[
            ['lifetime_impressions', 'lifetime_metric_numerator', 'today_impressions', 'today_metric_numerator']].sum()
        lifetime_impressions_dict = campaign_level_metrics.lifetime_impressions.to_dict()

        self.logger.info("total number of campaigns: {}".format(len(lifetime_impressions_dict.keys())))

        today_impressions_dict = campaign_level_metrics.today_impressions.to_dict()
        lifetime_metric_numerator_dict = campaign_level_metrics.lifetime_metric_numerator.to_dict()
        today_metric_numerator_dict = campaign_level_metrics.today_metric_numerator.to_dict()
        self.df.lifetime_impressions = self.df.campaign_id.map(lambda x: lifetime_impressions_dict[x])
        self.df.today_impressions = self.df.campaign_id.map(lambda x: today_impressions_dict[x])
        self.df.lifetime_metric_numerator = self.df.campaign_id.map(lambda x: lifetime_metric_numerator_dict[x])
        self.df.today_metric_numerator = self.df.campaign_id.map(lambda x: today_metric_numerator_dict[x])

        # print "The metrics have been converted from Banner level to Campaign level aggregation."
        self.logger.info("The metrics have been converted from Banner level to Campaign level aggregation.")


    def handle_programmatic_slices(self,**kwargs):
        pass

    def create_variables(self):
        self.logger.info("Time to create optimization variables")
        self.decision_variables = {}
        for row in self.df.itertuples():
            variable = str('x' + str((row).index))
            variable = pulp.LpVariable(str(variable), lowBound=0, upBound=(row).remaining, cat='Integer')
            self.decision_variables[(row).index] = variable

    def add_constraint(self,**kwargs):
        primary_keys = kwargs['primary_keys']
        index = kwargs['index_variable']
        inequality_dict_key = kwargs['inequality_dict_key']
        inequality_value = kwargs['inequality_value']
        is_tuple = kwargs['inequality_value']
        process_pool = kwargs['pool']
        soft_constraints_dict = kwargs.get('soft_constraints_dict')

        pk_dict = self.df.groupby(primary_keys)[index].apply(lambda x: x.tolist()).to_dict()
        ineq_dict = self.df[inequality_dict_key].drop_duplicates().groupby(primary_keys)[inequality_value].sum().to_dict()
        pk_list = self.solver.df[primary_keys].drop_duplicates().values.tolist()

        if is_tuple:
            list1 = [(pk_dict, ineq_dict, tuple(i),soft_constraints_dict) for i in pk_list]
        else:
            list1 = [(pk_dict, ineq_dict, i,soft_constraints_dict) for i in pk_list]

        constraints = process_pool.map(self.create_constraint, list1)
        #@TODO add validation all constrainsts have been added






