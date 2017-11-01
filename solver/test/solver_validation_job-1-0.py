# Run:
# python solver_plan_validation.py <SolverFilter Input Folder> <the Solver output tsv file to check> <Full s3 path for output of the validation job> <validatation output file name>
#

import sys
import glob
import os
import pandas as pd
import time

current_hour=time.gmtime().tm_hour

files = glob.glob("/mnt/solvertmp/*part*")
for f in files:
    os.remove(f)
try:
    outputFile_final=sys.argv[4]
    if '.tsv' in outputFile_final:
        pass
    else:
        outputFile = outputFile_final+'.tsv'
except:
    print 'Output file name not specified'
    sys.exit(1)

try:
    S3InputPath=sys.argv[1]
    inputfile_path_broken=S3InputPath.split('/')
    inputfile_path_broken.remove('')
    inputfile_bucket=inputfile_path_broken.pop(1)
    inputfile_path_broken=inputfile_path_broken[1:]
    S3InputfilePath='/'.join(inputfile_path_broken)
except:
    print 'Input file path not specified'
    sys.exit(0)

# Extract S3 Outputfile Path
try:
    S3OutputfilePath=sys.argv[2]
    outputfile_path_broken=S3OutputfilePath.split('/')
    outputfile_path_broken.remove('')
    outputfile_bucket=outputfile_path_broken.pop(1)
    outputfile_path_broken=outputfile_path_broken[1:]
    S3OutputfilePath='/'.join(outputfile_path_broken)
except:
    print 'Output file path not specified'
    sys.exit(0)

# Extract S3 Output Path
try:
    S3OutputPath=sys.argv[3]
    output_path_broken=S3OutputPath.split('/')
    output_path_broken.remove('')
    output_bucket=output_path_broken.pop(1)
    output_path_broken=output_path_broken[1:]
    S3OutputPath='/'.join(output_path_broken)
except:
    print 'Output path not specified'
    sys.exit(0)

try:
    current_hour=sys.argv[5]
except:
    print 'current_hour is missing ; setting to the default(time.gmtime().tm_hour)'

try:
    today_date=sys.argv[6]
except:
    print 'today_date is missing ; setting to the default(time.gmtime())'    

import boto
aws_connection=boto.s3.connect_to_region('us-west-2')
inputfile_bucket = aws_connection.get_bucket(inputfile_bucket)
outputfile_bucket = aws_connection.get_bucket(outputfile_bucket)
output_bucket= aws_connection.get_bucket(output_bucket)
    
LOCAL_PATH='/mnt/solvertmp/'
try:
    os.mkdir('/mnt/solvertmp/')
except:
    pass

inputfile_bucket_list = inputfile_bucket.list(S3InputfilePath, "/")
for key in inputfile_bucket_list:
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


try:
    input1.columns=['hour','zone_id','region','country','dma','start_time','end_time','forecast','kpi_forecast'\
        ,'banner_id','campaign_id','limitation','lifetime_impressions','today_impressions','needed_daily_quota','skip_daily_goal','weight','priority','units'\
        ,'experiment_precentage','lifetime_numerator_metric','today_numerator_metric','kpi_type','kpi_goal','is_programmatic']

except:
    print 'Did not get expected number of columns. Check the input data'
    sys.exit(1)

input1=input1[input1.zone_id>0]
#get the solver output tsv

try:
    solverOutputFile=LOCAL_PATH+'output_file.tsv'
    outputFile_key=outputfile_bucket.get_key(S3OutputfilePath)
    outputFile_key.get_contents_to_filename(solverOutputFile)
    solver_output=pd.read_csv(solverOutputFile,sep='\t')
except:
    print 'Error reading in solver output tsv file'
    sys.exit(1)

solver_output=solver_output[solver_output.zone_id>1]
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

def check_campaign_presence():
    input_list=input1.campaign_id.unique().tolist()
    output_list=solver_output.campaign_id.unique().tolist()
    control=True
    for campaign in input_list:
        if campaign not in output_list:
            control=False
    return control
    
def check_zone_presence():
    input_list=input1.zone_id.unique().tolist()
    output_list=solver_output.zone_id.unique().tolist()
    control=True
    for zone in input_list:
        if zone not in output_list:
            control=False
    return control

def check_campaign_unit_limit():
    #campaign_today_impressions=input1[['campaign_id','today_impressions']].drop_duplicates().groupby('campaign_id')['today_impressions'].sum().to_dict()
    solver_buy_at_most=solver_output.groupby('campaign_id')['buy_at_most'].sum().to_dict()
    campaign_lifetime = input1[['campaign_id','lifetime_impressions']].drop_duplicates().set_index('campaign_id').to_dict()['lifetime_impressions']
    campaign_ordered=input1[['campaign_id','units']].drop_duplicates().set_index('campaign_id').to_dict()['units']
    control=True
    for campaign in solver_output.campaign_id.unique().tolist():
        try:
            but_at_most=solver_buy_at_most[campaign]
        except:
            but_at_most=0
        try:
            lifetime=campaign_lifetime[campaign]
        except:
            lifetime=0
        #try:
        #    today=campaign_today_impressions[campaign]
        #except:
        #    today=0
        try:
            ordered=campaign_ordered[campaign]
        except:
            ordered=0
        #if but_at_most+lifetime+today>ordered*1.1:
        if but_at_most+lifetime>ordered*1.1:
            control=False
    return control

def return_time_tuple(x,tup):
    try:
        x=time.gmtime(x)
    except:
        print 'Error converting Epoch time to TimeStamp; Check start_time and end_time columns'
        sys.exit(1)
    #if tup!=(x.tm_year,x.tm_mon,x.tm_mday):
    if tup!=(x.tm_year):
        return False
    return True


def check_time():
    try:
        today_date=time.strptime(today_date,'%Y-%m-%d')
    except:
        print 'Supplied date not in standard format. Date set to time.gmtime()'
        #sys.exit(1)
    	today_date=time.gmtime()
    year_= today_date.tm_year
    month_ = today_date.tm_mon
    day_ = today_date.tm_mday
    #tup=(year_,month_,day_)
    tup=(year_)
    start_test=solver_output.start_time.map(lambda x:return_time_tuple(x,tup))
    end_test=solver_output.end_time.map(lambda x:return_time_tuple(x,tup))
    if (False in start_test.unique().tolist()) | (False in end_test.unique().tolist()):
        return False
    return True

def check_rate(x):
    if x==0:
        control=False

def check_zones():
    frame=solver_output.groupby('zone_id')[['forecast','buy_at_most']].sum()
    frame['rate']=frame['buy_at_most']/frame['forecast']
    control=True
    frame.rate.map(lambda x:check_rate(x))
    return control

def check_empty():
    if solver_output.shape[1]==0:
        return False
    return True

def check_presence_of_hour(start,end):
    start=time.gmtime(start).tm_hour
    end=time.gmtime(end).tm_hour
    return current_hour in range(start,end+1)

def check_time_presence():
    return True in solver_output.apply(lambda x:check_presence_of_hour(x['start_time'],x['end_time']),axis=1).unique().tolist()

list_of_checks=[check_campaign_presence,check_campaign_unit_limit,check_time,check_zones,check_zone_presence,check_empty,check_time_presence]
newlist=[]
convert_result={}
for checks in list_of_checks:
    if checks.__name__ in ['check_campaign_presence','check_zones','check_zone_presence']:
        convert_result[checks]={True:'Passed',False:'Warning'}
    else:
        convert_result[checks]={True:'Passed',False:'Failed'}

for checks in list_of_checks:
    newlist.append((checks.__name__,convert_result[checks][checks()]))

result=pd.DataFrame(newlist,columns=['check','result'])

from boto.s3.key import Key
k = Key(output_bucket)
result.to_csv(outputFile_final,index=False,sep='\t')

key_name = outputFile_final.replace("/mnt/solvertmp/", "")
full_key_name = os.path.join(S3OutputPath, key_name)
k = output_bucket.new_key(full_key_name)
k.set_contents_from_filename(outputFile_final)

# clean up
files = glob.glob("/mnt/solvertmp/*part*")
for f in files:
    os.remove(f)

#this is the code that prints the output and exits if some tests have failed. Irrespective of this the output is written to the file above
for row in newlist:
    print row[0],'\t',row[1]
    if row[1]=='Failed':
        print 'The above test failed and the job was terminated'
    sys.exit(1)
