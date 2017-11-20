import logging
import time
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
import multiprocessing

from pulp import *
from pulp import solvers
import pandas as pd
import numpy as np
import BaseSolver


start = time.time()
t = time.gmtime().tm_hour
log_file = 'solver_logger_{}.log'.format(t)
logging.basicConfig(
                    format='%(asctime)-32s  %(levelname)s  %(filename)22s %(funcName)14s :  %(message)10s',
                    level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(10)
solver = BaseSolver.BaseSolver("solver.csv",
                 "output.csv",
                 logger,
                 "max",
                 "performance_test")

#schema = ['hour_bucket','campaign_id','zone_id','region','country','requests','ctr','remaining']


def create_constraint(args):
    decision_vars_dict = args[0]
    target = args[1]
    values= args[2]
    vars = decision_vars_dict[values]
    return (vars,target[values])

def create_soft_constraint(args):
    decision_vars_dict = args[0]
    target = args[1]
    values= args[2]
    vars = decision_vars_dict[values]
    if target[values] > 100:
        c = LpAffineExpression([(decision_variables2[i], 1) for i in vars])
        inequality_fillrate = target[values] / 5
        logger.debug("zone_id: {}".format(str(values)))
        constraint = LpConstraint(e=c, sense=LpConstraintGE, name='elc1_' + str(values), rhs=inequality_fillrate)
        solver.solver_model.extend(constraint.makeElasticSubProblem(penalty=inequality_fillrate * 0.2, proportionFreeBound=0))

    return 1

if __name__ == "__main__":

    logger.info("Runnig solver, going to fetch the data...")



    solver.get_input(is_spark=False)
    solver.df = solver.df.loc[solver.df['zone_id']>0]

    logger.info("total slices (variables): {}".format(solver.df.shape[0]))
    logger.info("going to preprocess the slices...")
    solver.df['features'] = solver.df.region.astype(str).str.cat(solver.df.country.astype(str), sep='|')

    campaign_level_metrics = solver.df[
        ['campaign_id', 'banner_id', 'lifetime_impressions', 'lifetime_metric_numerator', 'today_impressions',
         'today_metric_numerator']].drop_duplicates() \
        .groupby('campaign_id')[
        ['lifetime_impressions', 'lifetime_metric_numerator', 'today_impressions', 'today_metric_numerator']].sum()
    lifetime_impressions_dict = campaign_level_metrics.lifetime_impressions.to_dict()
    today_impressions_dict = campaign_level_metrics.today_impressions.to_dict()

    logger.info("total number of campaigns: {}".format(len(lifetime_impressions_dict.keys())))
    solver.df.lifetime_impressions = solver.df.campaign_id.map(lambda x: lifetime_impressions_dict[x])
    solver.df.today_impressions = solver.df.campaign_id.map(lambda x: today_impressions_dict[x])


    logger.info("calculate remaining impressions...")
    solver.df['remaining'] = np.floor(np.maximum(0,
                                              np.where(solver.df['skip_daily_goal'] == True,
                                                       solver.df['units'] - solver.df['lifetime_impressions'],
                                                       solver.df['needed_daily_quota'] - solver.df['today_impressions'])))


    step1 = time.time()
    logger.info("Time to create optimization variables")
    decision_variables2 = {}
    for row in solver.df.itertuples():
        variable = str('x' + str((row).index))
        variable = pulp.LpVariable(str(variable), lowBound=0, upBound=(row).remaining, cat='Integer')  # make variable binary
        decision_variables2[(row).index]= variable

    step2 = time.time()
    logger.info(step2 - step1)
    logger.info('Total number of decision variables: ' + str(len(decision_variables2)))

    logger.info("building first constraint")
    zone_hour = solver.df.groupby(['zone_id', 'hour'])['index'].apply(lambda x: x.tolist()).to_dict()
    zone_remaining_dict = solver.df[['zone_id', 'hour','features','forecast']].drop_duplicates().groupby(['zone_id', 'hour'])['forecast'].sum().to_dict()
    list1 = solver.df[['zone_id','hour']].drop_duplicates().values.tolist()
    list2 = [(zone_hour,zone_remaining_dict,tuple(i)) for i in list1]


    pool = Pool()
    #pool = ThreadPool()
    #pool.map(create_constraint, list2)

    constraints = pool.map(create_constraint, list2)
    for vars,inequality in constraints:
        lpvars = [decision_variables2[i] for i in vars]
        solver.solver_model+= lpSum(lpvars) <= inequality

    logger.info("building second constraint")
    campaigns = solver.df.campaign_id.unique().tolist()
    remaining_dict = solver.df[['campaign_id', 'remaining']].drop_duplicates().set_index('campaign_id')['remaining'].to_dict()
    campaigns_index = solver.df.groupby(['campaign_id'])['index'].apply(lambda x: x.tolist()).to_dict()
    list2 = [(campaigns_index, remaining_dict, i) for i in campaigns]
    multiprocessing.Process(target=create_constraint)
    constraints = pool.map(create_constraint, list2)
    for vars,inequality in constraints:
        lpvars = [decision_variables2[i] for i in vars]
        solver.solver_model+= lpSum(lpvars) <= inequality


    logger.info("building elastic constraint")
    zones = solver.df.zone_id.unique().tolist()
    zone_remaining = solver.df.groupby(['zone_id'])['forecast'].apply(lambda x: x.tolist()).to_dict()
    zone_remaining_dict = solver.df[['zone_id', 'hour','features','forecast']].drop_duplicates().groupby(['zone_id'])['forecast'].sum().to_dict()
    zone_index = solver.df.groupby(['zone_id'])['index'].apply(lambda x: x.tolist()).to_dict()
    list2 = [(zone_index, zone_remaining_dict,i) for i in zones]
    constraints = pool.map(create_soft_constraint, list2)
    logger.info("Runnigs solver...")

    solver.solver_model.writeLP("solver_c1_elc1.lp")

    solver.solver_model += lpSum(decision_variables2.values())
    solver.solver_model.solve(solvers.GLPK_CMD())

    # Each of the variables is printed with it's resolved optimum value


    results = [{'index': i, 'buy_at_most': decision_variables2[i].value()} for i in decision_variables2.keys()]
    solver.df = pd.merge(solver.df,pd.DataFrame(results),on='index',how='left')
    solver.df.loc[solver.df['buy_at_most']>0].to_csv('results.csv',index=False)




