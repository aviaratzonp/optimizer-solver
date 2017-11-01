__author__ = 'kraghavan'
##Temp line for yosh

from joblib import Parallel, delayed
from pulp import *
import time
import json
import sys
import os
import glob
import pandas as pd
from optparse import OptionParser
import math


start = time.time()

#defult time to current_hour
t = time.gmtime().tm_hour

# clean up
files = glob.glob("/mnt/solvertmp/*")
for f in files:
    os.remove(f)

parser=OptionParser()
parser.add_option("-i","--input",dest="input_file_path",action="store",type="string",help="S3 location of the input to the solver",default=None)
parser.add_option("-o","--output",dest="output_file_path",action="store",type="string",help="S3 location of the output path of the solver",default=None)
parser.add_option("-n","--name",dest="filename",action="store",type="string",help="Filename of the output of the solver",default=None)
parser.add_option("-f","--filetype",dest="file_type",action="store",type="string",help="Type of output. json/tsv",default="json")
parser.add_option("-s","--strategy_id",dest="strategy_id",action="store",type="int",help="Strategy Id. Default set to -1.",default=-1)
parser.add_option("-t","--time",dest="current_hour",action="store",type="int",help="Current hour in UTC. Deafult is taken from server time (in UTC).",default=None)
parser.add_option("-d","--default_percentage",dest="default_percentage",type="float",action="store",help="Percetage of default per campaign as a float.Default set at 0.20(20%)",default=0.2)
parser.add_option("-r","--reactive_output",dest="reactive_folder",type="string",action="store",help="S3 location of the output of the reactive job",default=None)

(options, args) = parser.parse_args()
# Input and output files - Need to extract this from argv still

# s3_input_path="runtime/20160822_123608/avalvility_filter/"
# output_file="solver_output.json"
# s3_output_path="runtime/20160822_123608/solver"

# Extract S3 Input Path
try:
    S3InputPath=options.input_file_path
    input_path_broken=S3InputPath.split('/')
    if '' in input_path_broken:
        input_path_broken.remove('')
    input_bucket=input_path_broken.pop(1)
    input_path_broken=input_path_broken[1:]
    S3InputPath='/'.join(input_path_broken)+'/'
except:
    print 'Input path not specified or incorrect. Check help by python script.py --help'
    sys.exit(0)

# Extract S3 Output Path
try:
    S3OutputPath=options.output_file_path
    output_path_broken=S3OutputPath.split('/')
    if '' in output_path_broken:
        output_path_broken.remove('')
    output_bucket=output_path_broken.pop(1)
    output_path_broken=output_path_broken[1:]
    S3OutputPath='/'.join(output_path_broken)
except:
    print 'Output path not specified or incorrect. Check help by python script.py --help'
    sys.exit(0)

try:
    ReactiveS3Path = options.reactive_folder
    reactive_path_broken=ReactiveS3Path.split('/')
    if '' in reactive_path_broken:
	reactive_path_broken.remove('')
    reactive_bucket=reactive_path_broken.pop(1)
    reactive_path_broken=reactive_path_broken[1:]
    ReactiveS3Path='/'.join(reactive_path_broken)+'/'
    reactive=True
    if ReactiveS3Path==None:
        print "Reactive job folder not specified. Input will not be filtered."
	reactive=False
except:
    print "Reactive job folder not specified. Input will not be filtered."
    reactive=False

# Extract S3 Output Path
try:
    output_file=options.filename
    if ('.' in output_file) or ('/' in output_file):
        print "Invalid output file"
        sys.exit(1)
except:
    print 'Output file not specified'
    sys.exit(1)


output_format=options.file_type
if output_format in ['json','tsv']:
    if output_format=='json':
        output_file=output_file+'.json'
    else:
        output_file=output_file+'.json'
        output_file_tsv=output_file.replace('.json','')+'.tsv'
else:
    print "Output format can be either 'json' or 'tsv'"
    sys.exit(1)



    
try:
    strategy_id=options.strategy_id
    if strategy_id==-1:
        print 'Strategy id is missing or not in the correct format(int); setting to the default(-1).'
except:
    print "Error setting the strategy Id"
    exit(1)

	
try:
    current_hour=options.current_hour
    if current_hour==None:
        print "current_hour is missing ; setting to the default(time.gmtime().tm_hour)"
        current_hour=t
except:
    print "Error setting current hour."
    exit(1)

try:
    default_percentage=options.default_percentage
    print "Default percetage set to :",default_percentage*100," %"
except:
    print "Error setting default percentage."
    exit(1)


solver_model = LpProblem('The Maximization Problem', LpMaximize)

# Setup S3 connection
import boto
aws_connection=boto.s3.connect_to_region('us-west-2')
input_bucket = aws_connection.get_bucket(input_bucket)
output_bucket = aws_connection.get_bucket(output_bucket)



LOCAL_PATH='/mnt/solvertmp/'
try:
    os.mkdir('/mnt/solvertmp/')
except:
    pass

if reactive:
    reactive_bucket = aws_connection.get_bucket(reactive_bucket)
    reactive_bucket_list=reactive_bucket.list(ReactiveS3Path,"/")
    for key in reactive_bucket_list:
        keyName=str(key.name).replace("/","_")
	OutputFile=LOCAL_PATH+'reactive_job/'+keyName
	if '.csv' in OutputFile:
	    OutputFile=LOCAL_PATH+'reactive_job'
	    try:
	        key.get_contents_to_filename(OutputFile)
	    except OSError:
	        # check if dir exists
                if not os.path.exists(OutputFile):
                    os.mkdir(OutputFile)
    reactiveFileList=glob.glob("/mnt/solvertmp/*reactive_job*")
    #Read the reactive job output into a dataframe
    try:
        reactive_output=pd.DataFrame()
	for currentFile in reactiveFileList:
	    try:
	        df=pd.read_csv(currentFile,header=None)
	    	reactive_output=reactive_output.append(df,ignore_index=True)
    	    except:
		pass
	reactive_output.columns=['zone_id','requests']
    except:
	print 'Unable to read reactive output'
	sys.exit(1)

	
input_bucket_list = input_bucket.list(S3InputPath, "/")
for key in input_bucket_list:
    keyName = str(key.name).replace("/", "_")
    Outputfile = LOCAL_PATH + keyName
    try:
        key.get_contents_to_filename(Outputfile)
    except OSError:
        # check if dir exists
        if not os.path.exists(Outputfile):
            os.mkdir(Outputfile)

fileList = glob.glob("/mnt/solvertmp/*part*")

# Read Downloaded files into Dataframe
try:
    input1 = pd.DataFrame()
    for currentfile in fileList:
        try:
            df = pd.read_csv(currentfile,sep="\t",header=None)
            input1 = input1.append(df, ignore_index=True)
        except:
            pass
except:
    print 'Unable to read in the Input'
    sys.exit(1)

step1=time.time()


print "Time to read in data and create dataframe"
print step1-start
print 'Current hour:'+str(current_hour)

try:
    input1.columns=['hour','zone_id','region','country','dma','start_time','end_time','forecast','kpi_forecast'\
        ,'banner_id','campaign_id','limitation','lifetime_impressions','today_impressions','needed_daily_quota','skip_daily_goal','weight','priority','units'\
        ,'experiment_precentage','lifetime_metric_numerator','today_metric_numerator','kpi_type','kpi_goal','is_programmatic']
except:
    print 'Did not get expected number of columns. Check the input data'
    sys.exit(1)
	
####Filter out zones from reactive jobs which do not have traffic
if (reactive and current_hour>7):
    try:
    	total_zones_forecast=input1[input1.zone_id>0][['zone_id','hour','country','region','dma','forecast']].drop_duplicates().groupby('zone_id').forecast.sum().reset_index()
    	percentile_limit=total_zones_forecast.forecast.describe()['75%']
    	big_zones_list=total_zones_forecast[total_zones_forecast.forecast>percentile_limit].zone_id.unique()
    	reactive_zones=reactive_output.zone_id.unique().tolist()
	filter_zones=[]
	for zone in big_zones_list:
	    if zone not in reactive_zones:
	        filter_zones.append(zone)
	if len(filter_zones) >0:
	    print 'The reactive job filtered out these zones: ',filter_zones
	    input1 = input1[~input1.zone_id.isin(filter_zones)]
    except:
	print "unable to filter reactive zones"
	sys.exit(1)


#####
##Filter out rows with daily goal==0 and skip_daily_goal=0
### If one of the 2 columns is 0 with the other non 0 the row is still retained.

input1=input1[(input1.needed_daily_quota!=0) | (input1.skip_daily_goal!=False)]
###Make metrics camapign level
campaign_level_metrics=input1[['campaign_id','banner_id','lifetime_impressions','lifetime_metric_numerator','today_impressions','today_metric_numerator']].drop_duplicates()\
    .groupby('campaign_id')[['lifetime_impressions','lifetime_metric_numerator','today_impressions','today_metric_numerator']].sum()
lifetime_impressions_dict=campaign_level_metrics.lifetime_impressions.to_dict()
today_impressions_dict=campaign_level_metrics.today_impressions.to_dict()
lifetime_metric_numerator_dict=campaign_level_metrics.lifetime_metric_numerator.to_dict()
today_metric_numerator_dict=campaign_level_metrics.today_metric_numerator.to_dict()
input1.lifetime_impressions=input1.campaign_id.map(lambda x:lifetime_impressions_dict[x])
input1.today_impressions=input1.campaign_id.map(lambda x:today_impressions_dict[x])
input1.lifetime_metric_numerator=input1.campaign_id.map(lambda x:lifetime_metric_numerator_dict[x])
input1.today_metric_numerator=input1.campaign_id.map(lambda x:today_metric_numerator_dict[x])
print "The metrics have been converted from Banner level to Campaign level aggregation."

#Split the frame into regular slices and default slices and programmatic slices
def_frame=input1[(input1.zone_id<=0)]
input1=input1[(input1.zone_id>0)&(input1.is_programmatic==False)]

## Filter out the hours that have already occurred
def change_hours_to_list(x):
    x=map(int,x.split(','))
    return ','.join(map(str,range(x[0],x[1]+1)))
input1['hour']=input1['hour'].map(lambda x:change_hours_to_list(x))

try:   
    hours_list=input1.hour.unique().tolist()
    hours_to_keep=[]
    for slot in hours_list:
        if int(slot.split(',')[-1])>=current_hour:
            hours_to_keep.append(slot)
except:
    print "Error calculating remaining hours"
    sys.exit(1)
    
input1 = input1[input1.hour.isin(hours_to_keep)]
################

def condense_feature(x):
    return str(x['dma'])+'|'+str(x['country'])+'|'+str(x['region'])

input1['features']=input1.apply(lambda x:condense_feature(x),axis=1)
def_frame['features']=def_frame.apply(lambda x:condense_feature(x),axis=1)
def calculate_remaining(x):
    if x['skip_daily_goal']:
        return int(max(0,x['units']-x['lifetime_impressions'])*1.0)
    return int(max(0,x['needed_daily_quota']-x['today_impressions'])*1.0)
    

input1['remaining']=input1.apply(lambda x:calculate_remaining(x),axis=1)
def_frame['remaining']=def_frame.apply(lambda x:calculate_remaining(x),axis=1)
####Change forecast if part of the hour has already occured
def change_forecast(x):
    hour_str=x['hour']
    hour = map(int,hour_str.split(','))
    if current_hour in hour:
        factor=(hour_str.count(',')+1-map(int,hour_str.split(',')).index(current_hour))/float(hour_str.count(',')+1)
        return int(round(x['forecast']*factor))
    return x['forecast']
input1['forecast']=input1.apply(lambda x:change_forecast(x),axis=1)
######
def change_kpi_type(x):
    if x['kpi_goal']==0:
        return "Pacing"
    return x['kpi_type']

input1['kpi_type']=input1.apply(lambda x:change_kpi_type(x),axis=1)

campaigns=input1.campaign_id.unique().tolist()

############################
######Default slices########
max_end_time=input1['end_time'].max()
min_start_time=input1['start_time'].min()
remaining_hours=24-current_hour
temp_hour=0
list_of_times=[]
while temp_hour<remaining_hours:
    sabtracting_hour_factor=3600*temp_hour
    new_end_time=max_end_time-sabtracting_hour_factor
    list_of_times.append((new_end_time-3599,new_end_time))
    temp_hour+=1

input2=def_frame
try:
    campaign_divisor_dict=input2.groupby('campaign_id')['banner_id'].nunique().to_frame().banner_id.to_dict()
except:
    print "Error making defaults.  Unable to  count banners per campaign"
    exit(1)

input2['buy_at_most']=input2.apply(lambda x:int(int(x['remaining']*default_percentage)/(campaign_divisor_dict[x['campaign_id']]*remaining_hours)),axis=1)
if input2.shape[0]!=input2.banner_id.nunique():
    print "Error creating defaults. Number of Banner_ids not matching number of default slices"
    exit(1)
else:
    print "Successfully created default slices per hour."

try:
    input3=pd.DataFrame()
    for row in list_of_times:
        temp_frame=input2.copy()
        temp_frame.start_time=row[0]
        temp_frame.end_time=row[1]
        input3 = input3.append(temp_frame, ignore_index=True)
    print 'Successfully aggregated the default slices.'
except:
    print 'Unable to aggregate the default slices.'
    sys.exit(1)
	
####Changing the programmatic default slices
def change_programmatic_buy_at_most(x):
    if x['is_programmatic']==True:
	return -1
    return x['buy_at_most']
def change_prpgrammatic_zone_id(x):
    if x['is_programmatic']==True:
	return -2
    return x['zone_id']
try:
    input3['buy_at_most']=input3.apply(lambda x:change_programmatic_buy_at_most(x),axis=1)
    input3['zone_id']=input3.apply(lambda x:change_prpgrammatic_zone_id(x),axis=1)
except:
    print "Unable to change programmatic buy at most"
    sys.exit(1)
################

#### Creating CTR dict
optimized_kpis=['CTR','Interaction']
kpi_forecast_dict={}
def create_kpi_forecast_dict(dict1,slice):
    dict1[slice['zone_id'],slice['hour'],slice['features'],slice['banner_id']]=slice['kpi_forecast']
input1[input1['kpi_type'].isin(optimized_kpis)][['zone_id','hour','features','banner_id','kpi_forecast']].drop_duplicates().apply(lambda x:create_kpi_forecast_dict(kpi_forecast_dict,x),axis=1)

####Creating variable names
def create_variable_names(dict1,slice):
    dict1[slice['zone_id'],slice['banner_id'],slice['campaign_id'],slice['hour'],slice['features']]=slice['index']

variable_name_dict={}
input1[['zone_id','campaign_id','banner_id','hour','features']].drop_duplicates().reset_index(drop=True).reset_index().apply(lambda x:create_variable_names(variable_name_dict,x),axis=1)

## this function creates the variable dict
def create_variable_dict(dict_1,slice):
    dict_1[slice['zone_id'],slice['banner_id'],slice['campaign_id'],slice['hour'],slice['features']]=\
        LpVariable("x_"+str(variable_name_dict[slice['zone_id'],slice['banner_id'],slice['campaign_id'],slice['hour'],slice['features']])\
                   ,lowBound=0,upBound=int(slice['remaining']),cat='Integer')

variables = {}
test=[]

input1.apply(lambda x: create_variable_dict(variables,x),axis=1)

print "Time to create optimization variables"
step2=time.time()
print step2-step1

####Calculate balances for soft constraint
###FOR Now Interaction and CTR soft constraints have the same penalty. This means both interaction and CTR are equally important
ctr_balance=float(input1[input1.kpi_type.isin(optimized_kpis)][['campaign_id','remaining']].drop_duplicates().remaining.sum())/input1[(input1.skip_daily_goal==False)&(input1.is_programmatic==False)][['campaign_id','remaining']].drop_duplicates().remaining.sum()
programmatic_balance=float(input1[input1.is_programmatic==True][['campaign_id','remaining']].drop_duplicates().remaining.sum())/input1[['campaign_id','remaining']].drop_duplicates().remaining.sum()
programmatic_balance=ctr_balance*programmatic_balance
boolean_coeff_dict={True:programmatic_balance,False:1}
temp_frame=input1[['campaign_id','is_programmatic']].drop_duplicates()
temp_frame['multiplier']=temp_frame.is_programmatic.map(lambda x:boolean_coeff_dict[x])
multiplier_dict=temp_frame.set_index('campaign_id')['multiplier'].to_dict()
max_daily_goal=input1.needed_daily_quota.max()
fill_rate_penalty = max_daily_goal*ctr_balance*0.20

############SOFT CONSTRAINTS########
#############CONSTRAINTS 1 and 2###########
#############This constraint ensures that for a given zone the fill rate is atleast 50% this is elastic#########
n=time.time()
#function to create the zone level availability dict
def create_zone_avails_total_dict(dict1,slice):
    if (slice['zone_id'],slice['hour']) in dict1.keys():
        print "Error2"
    else:
        dict1[slice['zone_id'],slice['hour']]=slice['forecast']

zone_fill_rate_dict={}
input1.groupby(['zone_id','hour'])['forecast'].sum().to_frame().reset_index().apply(lambda x:create_zone_avails_total_dict(zone_fill_rate_dict,x),axis=1)

def func_try(combo):
    inequality=zone_fill_rate_dict[tuple(combo)]
    variable_cache=[]
    for combo2 in list2:
        if (combo[0],combo2[1],combo2[0],combo[1],combo2[2]) in variables:
            variable_cache.append((combo[0],combo2[1],combo2[0],combo[1],combo2[2]))
    return (variable_cache,LpConstraintGE,fill_rate_penalty,inequality)

list1=input1[['zone_id','hour']].drop_duplicates().values.tolist()
list2=input1[['campaign_id','banner_id','features']].drop_duplicates().values.tolist()

soft_constraints1=Parallel(n_jobs=-1,backend='multiprocessing')(delayed(func_try)(combo) for combo in list1)

j=0
for row in soft_constraints1:
    variable_cache=map(variables.get,row[0])
    sense=row[1]
    penalty=row[2]
    inequality=row[3]
    #the line below sets the minimum fill rate . RIght now it is set t0 20%. This is obviously elastic and the penalty is
    #calculate above in fill_rate_penalty
    #temporary change to weaken the minimum fill rate peanlty
    #inequality_fillrate=int(round(inequality/5))
    inequality_fillrate=int(round(inequality/500))
    solver_model+=lpSum(variable_cache)<=inequality
    c = LpAffineExpression([(variable_cache[i],1) for i in range(len(variable_cache))])
    constraint = LpConstraint(e=c,sense=sense,name='elc1_'+str(j),rhs=inequality_fillrate)
    solver_model.extend(constraint.makeElasticSubProblem(penalty=penalty,proportionFreeBound=0))
    j += 1



print 'Time to build the fill rate elastic constraint constraint and the zone total avialability constraint: ',time.time()-n

############SOFT CONSTRAINTS########
#############CONSTRAINT 3 and 4###########
### daily goal per campaign cannot exceed 1.1 times the needed daily quota constraint
### and for every campaign the impressions should be atleast 100% the daily needed quota (this is elastic)
## the for loop below imposes two constraints

n=time.time()
temp_frame=input1[['campaign_id','needed_daily_quota']].drop_duplicates().merge(input1.groupby('campaign_id')['today_impressions'].sum().reset_index(),on='campaign_id',how='outer')
temp_frame['ndq_']=(temp_frame.needed_daily_quota*1.1)-temp_frame.today_impressions
temp_frame['ndq_']=temp_frame['ndq_'].map(lambda x:int(math.ceil(max(x,0))))
daily_goal_dict = temp_frame[['campaign_id','ndq_']].set_index('campaign_id')['ndq_'].to_dict()


#daily_goal_dict = input1[['campaign_id','needed_daily_quota']].drop_duplicates().set_index('campaign_id')['needed_daily_quota'].to_dict()
remaining_dict=input1[['campaign_id','remaining']].drop_duplicates().set_index('campaign_id')['remaining'].to_dict()
list2=input1[['zone_id','banner_id','hour','features']].drop_duplicates().values.tolist()
def constraint4_func(campaign):
    inequality = daily_goal_dict[campaign]
    inequality_remaining=remaining_dict[campaign]
    variable_cache=[]
    for combo2 in list2:
        if (combo2[0],combo2[1],campaign,combo2[2],combo2[3]) in variables:
            variable_cache.append((combo2[0],combo2[1],campaign,combo2[2],combo2[3]))
    return (variable_cache,LpConstraintGE,max_daily_goal*multiplier_dict[campaign],inequality,inequality_remaining)
soft_constraints2=Parallel(n_jobs=-1,backend='multiprocessing')(delayed(constraint4_func)(campaign) for campaign in campaigns)
j=0
for row in soft_constraints2:
    variable_cache=map(variables.get,row[0])
    sense=row[1]
    penalty=row[2]
    inequality=row[3]
    inequality_remaining=row[4]
    solver_model+=lpSum(variable_cache)<=inequality_remaining
    c = LpAffineExpression([(variable_cache[i],1) for i in range(len(variable_cache))])
    constraint = LpConstraint(e=c,sense=sense,name='elc2_'+str(j),rhs=inequality)
    solver_model.extend(constraint.makeElasticSubProblem(penalty=penalty,proportionFreeBound=0))
    j += 1
print 'Time to build campaign remaining constraint and the NDQ pacing elastic constraint: ',time.time()-n

############CONSTRAINT 5######################
## function to create the zone_level availability dict at feature level
n=time.time()
def create_zone_availability_dict(dict1,slice):
    dict1[slice['zone_id'],slice['hour'],slice['features']]=slice['forecast']
zone_avails_dict={}
input1[['zone_id','hour','features','forecast']].drop_duplicates().apply(lambda x:create_zone_availability_dict(zone_avails_dict,x),axis=1)

list1=input1[['zone_id','hour','features']].drop_duplicates().values.tolist()
list2=input1[['banner_id','campaign_id']].drop_duplicates().values.tolist()
def func1(combo):
    inequality=zone_avails_dict[tuple(combo)]
    variable_cache=[]
    for combo2 in list2:
        if (combo[0],combo2[0],combo2[1],combo[1],combo[2]) in variables:
            variable_cache.append((combo[0],combo2[0],combo2[1],combo[1],combo[2]))
    return (variable_cache,inequality)


total_cache=Parallel(n_jobs=-1,backend='multiprocessing')(delayed(func1)(combo) for combo in list1)
for row in total_cache:
    solver_model+=lpSum(map(variables.get,row[0]))<=row[1]

print 'Time to build the total zone availability forecast at feature level constraint: ',time.time()-n


#######CONSTRAINT 6##########
## function to create the campaign level total limitation constraint
n=time.time()
def create_campaign_limitation_dict(dict1,slice):
    if (slice['campaign_id'],slice['hour']) in dict1.keys():
        print "Error : Campaign remaining impressions not unique"
    else:
        dict1[slice['campaign_id'],slice['hour']]=slice['remaining']
campaign_upper_limit_dict={}
input1.groupby(['campaign_id','hour'])['remaining'].sum().to_frame().reset_index().apply(lambda x:create_campaign_limitation_dict(campaign_upper_limit_dict,x),axis=1)
## sum of all banner variables in a campaign not greater than the total remaining for that slice at each time slot
list1=input1[['campaign_id','hour']].drop_duplicates().values.tolist()
list2=input1[['zone_id','banner_id','features']].drop_duplicates().values.tolist()
def constraint2_func(combo):
    inequality=campaign_upper_limit_dict[tuple(combo)]
    variable_cache=[]
    for combo2 in list2:
        if (combo2[0],combo2[1],combo[0],combo[1],combo2[2]) in variables:
            variable_cache.append((combo2[0],combo2[1],combo[0],combo[1],combo2[2]))
    return (variable_cache,inequality)

total_cache=Parallel(n_jobs=-1,backend='multiprocessing')(delayed(constraint2_func)(combo) for combo in list1)
for row in total_cache:
    solver_model+=lpSum(map(variables.get,row[0]))<=row[1]
print 'Time to build (for now redundant) campaign hourly pacing constraint : ',time.time()-n

####Constraint 7#########
##function to create the kpi goal
print "Starting to build ctr and Interaction constraints"
n=time.time()
optimized_kpis=['CTR','Interaction']
kpi_campaigns=input1[((input1['kpi_type']=='CTR') | (input1['kpi_type']=='Interaction')) & (input1['is_programmatic']==0)]['campaign_id'].unique().tolist()
#goal_1_dict=input1[input1.kpi_type=='CTR'][['campaign_id','goal_1']].drop_duplicates().set_index('campaign_id')['goal_1'].to_dict()
#goal_2_dict=input1[input1.kpi_type=='CTR'][['campaign_id','goal_2']].drop_duplicates().set_index('campaign_id')['goal_2'].to_dict()
goal_dict=input1[input1['kpi_type'].isin(optimized_kpis)][['campaign_id','kpi_goal']].drop_duplicates().set_index('campaign_id')['kpi_goal'].to_dict()
list2=input1[input1['kpi_type'].isin(optimized_kpis)][['zone_id','banner_id','hour','features']].drop_duplicates().values.tolist()

def constraint7_func(campaign):
    #upper_bound=goal_2_dict[campaign]
    #lower_bound=goal_1_dict[campaign]
    lower_bound=goal_dict[campaign]
    variable_cache=[]
    kpi_cache=[]
    for combo2 in list2:
        if (combo2[0],combo2[1],campaign,combo2[2],combo2[3]) in variables:
            variable_cache.append((combo2[0],combo2[1],campaign,combo2[2],combo2[3]))
            kpi_cache.append((combo2[0],combo2[2],combo2[3],combo2[1]))
        #return (variable_cache,upper_bound,lower_bound,ctr_cache)
    return (variable_cache,lower_bound,kpi_cache)
total_cache=Parallel(n_jobs=-1,backend='multiprocessing')(delayed(constraint7_func)(campaign) for campaign in kpi_campaigns)

j=0
penalty1=max_daily_goal*ctr_balance
#penalty1= int(input1[input1['kpi_type']=='CTR'].needed_daily_quota.max()*0.5)
#penalty2= int(input1[input1['kpi_type']=='CTR'].needed_daily_quota.max()*0.1)
max_remaining=input1.remaining.max()
for row in total_cache:
    variable_cache=map(variables.get,row[0])
    #ctr_cache=map(ctr_forecast_dict.get,row[3])
    kpi_cache=map(kpi_forecast_dict.get,row[2])
    #upper_bound=row[1]
    #lower_bound=row[2]
    lower_bound=row[1]
    lb1=int(lower_bound/3.0)
    lb2=int((lower_bound*2.0)/3.0)
    lb3=lower_bound
    penalty2=int(penalty1/3.0)
    ###this expression is the same as :((sum of coeefficient* buy_at_most)/(sum of buy_at_most))>=goal1
    ### Once we move the vairables to one side it looks like : summation((coefficient-goal1)*buy_at_most)>=0
    c1 = LpAffineExpression([(variable_cache[i],kpi_cache[i]-lb1) for i in range(len(variable_cache))])
    constraint1 = LpConstraint(e=c1,sense=LpConstraintEQ,name='elc3_1_'+str(j),rhs=0)
    c2 = LpAffineExpression([(variable_cache[i],kpi_cache[i]-lb2) for i in range(len(variable_cache))])
    constraint2 = LpConstraint(e=c2,sense=LpConstraintEQ,name='elc3_2_'+str(j),rhs=0)
    c3 = LpAffineExpression([(variable_cache[i],kpi_cache[i]-lb3) for i in range(len(variable_cache))])
    constraint3 = LpConstraint(e=c3,sense=LpConstraintEQ,name='elc3_3_'+str(j),rhs=0)
    solver_model.extend(constraint1.makeElasticSubProblem(penalty=penalty2,proportionFreeBoundList=[0,max_remaining*3]))
    solver_model.extend(constraint2.makeElasticSubProblem(penalty=penalty2,proportionFreeBoundList=[0,max_remaining*3]))
    solver_model.extend(constraint3.makeElasticSubProblem(penalty=penalty2,proportionFreeBoundList=[0,max_remaining*3]))
    ###this expression is the same as :((sum of coeefficient* buy_at_most)/(sum of buy_at_most))<=goal2
    ### Once we move the vairables to one side it looks like : summation((coefficient-goal2)*buy_at_most)<=0
    #c2 = LpAffineExpression([(variable_cache[i],ctr_cache[i]-upper_bound) for i in range(len(variable_cache))])
    #constraint = LpConstraint(e=c2,sense=LpConstraintEQ,name='elc4_'+str(j),rhs=0)
    #solver_model.extend(constraint.makeElasticSubProblem(penalty=penalty2,proportionFreeBoundList=[max_remaining,0]))
    j += 1
print "Time taken to build CTR and Interaction constraints : ",time.time()-n



solver_model+=lpSum(variables.values())

step3=time.time()
print "Total time to set up the optimization problem: "
print step3-step2

def run_solver(i):
    try:
        solver_model.solve(solvers.GLPK_CMD())
    except:
        if i<5:
            i+=1
            run_solver(i)
        else:
            print "GLPK solver error"
            sys.exit(1)
i=0
run_solver(i)

step4=time.time()
print 'time to solve the problem :'
print step4-step3




def reattach_results(slice):
    return variables[(slice['zone_id'],slice['banner_id'],slice['campaign_id'],slice['hour'],slice['features'])].value()

input1['buy_at_most']=input1.apply(lambda x:reattach_results(x),axis=1)
filtered_output= input1[input1.buy_at_most==0].shape[0]
print 'the number of rows which have 0 buy a most and which are filtered out are : ',filtered_output
input1=input1[input1.buy_at_most!=0]
#######********#######

###Attaching the deafults####
try:
    input1=pd.concat([input1,input3])
except:
    "Print error attaching the default slices and/or programmatic slices to the solver plan; check the number of columns"
    exit(1)

input1.columns=['hour','zone_id','region','country','dma','start_time','end_time','forecast','KPI_SCORE'\
        ,'banner_id','campaign_id','limitation','lifetime_impressions','today_impressions','needed_daily_quota','skip_daily_goal','weight','priority','units'\
        ,'experiment_precentage','lifetime_metric_numerator','today_metric_numerator','KPI_TYPE','kpi_goal','is_programmatic','features'\
		,'remaining','buy_at_most']

def change_kpi_score(x):
    if x['KPI_TYPE']=='CTR':
	return min(0.99,x['KPI_SCORE'])
    elif x['KPI_TYPE']=='Interaction':
	return min(0.99,x['KPI_SCORE']/0.01)
    return x['KPI_SCORE']

input1['KPI_SCORE']=input1.apply(lambda x:change_kpi_score(x),axis=1)

def create_predicates(x,list_c):
    d={}
    d['AND']=list()
    for column in list_c:
        var= str(x[column])
        if '!' in var:
            temp_dict={}
            if ',' in var:
                try:
                    temp_dict['in']=['body.features.'+column.replace("c_",""),map(int,var.strip('!').split(','))]
                except:
                    temp_dict['in']=['body.features.'+column.replace("c_",""),var.strip('!').split(',')]
            else:
                try:
                    temp_dict['==']=['body.features.'+column.replace("c_",""),int(var.strip('!'))]
                except:
                    temp_dict['==']=['body.features.'+column.replace("c_",""),var.strip('!')]
            dict_2={}
            dict_2['!']=[temp_dict]
            d['AND'].append(dict_2)
        else:
            temp_dict={}
            if ',' in var:
                try:
                    temp_dict['in']=['body.features.'+column.replace("c_",""),map(int,var.split(','))]
                except:
                    temp_dict['in']=['body.features.'+column.replace("c_",""),var.split(',')]
            else:
                try:
                    temp_dict['==']=['body.features.'+column.replace("c_",""),int(var)]
                except:
                    temp_dict['==']=['body.features.'+column.replace("c_",""),var]
            d['AND'].append(temp_dict)
    return d





def write_slices(x,list_c,predicate_c):
    temp_dict={}
    for columns in list_c:
        temp_dict[columns]=x[columns]
    temp_dict['predicates']=create_predicates(x,predicate_c)
    final_dict['slices'].append(temp_dict)

##This is the list of features to be included in the predicates
list_c1=['zone_id','dma','country','region']
## This is the list of features in the slices
l1 = ['start_time','end_time','banner_id','weight','buy_at_most','slice_id','zone_id','KPI_TYPE','KPI_SCORE','is_programmatic']


input2 = input1[['zone_id','dma','banner_id','weight','country','region','start_time','end_time','buy_at_most','KPI_TYPE','KPI_SCORE','is_programmatic']].drop_duplicates()
input2 = input2.reset_index(drop=True).reset_index()
current_timestamp=time.time().__int__().__str__()
input2['slice_id']=input2['index'].map(lambda x:str(x)+'_'+current_timestamp)

def splitDataFrameIntoSmaller(df, chunkSize = 10):
    listOfDf = list()
    numberChunks = len(df) // chunkSize + 1
    for i in range(numberChunks):
        listOfDf.append(df[i*chunkSize:(i+1)*chunkSize])
    return listOfDf

df_list=splitDataFrameIntoSmaller(input2)
#input2.apply(lambda x:write_slices(x,l1,list_c1),axis=1)

# write output to local file 'output_file'

from boto.s3.key import Key
k = Key(output_bucket)

if output_format=='json':
    list_of_final_dicts=[]
    for df in df_list:
        part=str(i)
        final_dict={}
        final_dict['buying_strategy_id']=strategy_id
        final_dict['slices']=[]
        df.apply(lambda x:write_slices(x,l1,list_c1),axis=1)
        list_of_final_dicts.append(final_dict)
    for final_dict in list_of_final_dicts:
        with open(output_file, "a") as json_file:
            json.dump(final_dict, json_file)
            json_file.write('\n')
else:
    input1.to_csv(output_file_tsv,index=False,sep='\t')
    list_of_final_dicts=[]
    for df in df_list:
        part=str(i)
        final_dict={}
        final_dict['buying_strategy_id']=strategy_id
        final_dict['slices']=[]
        df.apply(lambda x:write_slices(x,l1,list_c1),axis=1)
        list_of_final_dicts.append(final_dict)
    for final_dict in list_of_final_dicts:
        with open(output_file, "a") as json_file:
            json.dump(final_dict, json_file)
            json_file.write('\n')

# and Upload this file to S3
key_name = output_file.replace("/mnt/solvertmp/", "")
full_key_name = os.path.join(S3OutputPath, key_name)
k = output_bucket.new_key(full_key_name)
k.set_contents_from_filename(output_file)

try:
    key_name = output_file_tsv.replace("/mnt/solvertmp/", "")
    full_key_name = os.path.join(S3OutputPath, key_name)
    k = output_bucket.new_key(full_key_name)
    k.set_contents_from_filename(output_file_tsv)
except:
    pass

# clean up
files = glob.glob("/mnt/solvertmp/*")
for f in files:
    os.remove(f)
try:
    os.remove(output_file_tsv)
except:
    pass

try:
    os.remove(output_file)
except:
    pass

step5=time.time()
print "time to write the output :"
print step5-step4
