############################################################################################################################
#       Name        : US_Crashes_Analysis.py
#       Description : This code is devloped for analysis of Crashes Data for BCG Usecase
#       Execution   : spark-submit --master local[8] --deploy-mode client --properties-file /home/ishaan25/Documents/BGC_UseCase/Config/input_path.prop /home/ishaan25/Documents/BGC_UseCase/Code/US_Crashes_Analysis.py > /home/ishaan25/Documents/BGC_UseCase/Output/analysis_report.txt
#
#       Author      : Ishaan Bhatnagar
#       
#       Change Log  :
#       ------------------------------------------------------------------------------------------
#       Date                    Developer                Description
#       ------------------------------------------------------------------------------------------
#       25-SEP-2021             Ishaan Bhatnagar         Created
#
############################################################################################################################

import findspark
findspark.init()

import pyspark
import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

#Creating the Spark Session:
spark = SparkSession.builder.appName("CrashesAnalysis").getOrCreate()

#Reading Config File:
charges_file_path = spark.conf.get("spark.charges_file_path")
damages_file_path = spark.conf.get("spark.damages_file_path")
endorse_file_path = spark.conf.get("spark.endorse_file_path")
primary_person_file_path = spark.conf.get("spark.primary_person_file_path")
restrict_file_path = spark.conf.get("spark.restrict_file_path")
units_file_path = spark.conf.get("spark.units_file_path")

#Defining the log function:
def log(log_level, err_str):
    time = str(datetime.datetime.now())
    print(time + ' ' + log_level + ' Crashes_Analysis: ' + err_str + '\n')

#Reading the CSVs in DataFrames
df_charges = spark.read.csv(charges_file_path,header=True)
df_damages = spark.read.csv(damages_file_path,header=True)
df_endorse = spark.read.csv(endorse_file_path,header=True)
df_primary_person = spark.read.csv(primary_person_file_path,header=True)
df_restrict = spark.read.csv(restrict_file_path,header=True)
df_units = spark.read.csv(units_file_path,header=True)

log('INFO','EXECUTION STARTED')

########## Analytics 1: 
#Find the number of crashes (accidents) in which number of persons killed are male?

print("Analysis 1: Find the number of crashes (accidents) in which number of persons killed are male?")
print("Analysis 1 Solution:")

#Analysis:
#No of Acc = 182, Distinct Acc = 180 (In 2 Acc, 2-2 Males died in crash_id '15429998','15379024')
res1 = df_primary_person.where("PRSN_INJRY_SEV_ID = 'KILLED' and PRSN_GNDR_ID = 'MALE'").select(F.col("CRASH_ID")).distinct().count()
print(res1)

log('INFO','ANALYSIS 1 COMPLETED')


########## Analysis 2: 
#How many two wheelers are booked for crashes:

print("Analysis 2: How many two wheelers are booked for crashes")
print("Analysis 2 Solution:")
res2 = df_units.where("VEH_BODY_STYL_ID in ('MOTORCYCLE','POLICE MOTORCYCLE')").select(F.col("CRASH_ID")).distinct().count()
print(res2)

log('INFO','ANALYSIS 2 COMPLETED')


########## Analysis 3: 
#Which state has highest number of accidents in which females are involved? 

print("Analysis 3: Which state has highest number of accidents in which females are involved?")
print("Analysis 3 Solution:")

df_primary_person.where("PRSN_GNDR_ID = 'FEMALE'").groupby(F.col("DRVR_LIC_STATE_ID")).count().distinct().sort(F.col('count').desc()).show(1,False)

log('INFO','ANALYSIS 3 COMPLETED')


########## Analysis 4: 
#Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death

print("Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death")
print("Analysis 4 Solution:")

df_req_units = df_units.select(F.col("VEH_MAKE_ID"),F.col("CRASH_ID"))
#df_req_units.show(5,False)
df_injured_person = df_primary_person.where("PRSN_INJRY_SEV_ID in ('KILLED','NON-INCAPACITATING INJURY','INCAPACITATING INJURY','POSSIBLE INJURY')").select(F.col('CRASH_ID')).distinct()
#df_injured_person.show(5,False)
join_cond = [df_injured_person['crash_id'] == df_req_units['crash_id']]
df_units_with_injury = df_req_units.join(df_injured_person, on = join_cond, how='inner').drop(df_req_units['crash_id'])
df_req = df_units_with_injury.groupby(F.col("VEH_MAKE_ID")).count().sort(F.col('count').desc())
df_req_rnk = df_req.withColumn("crash_cnt",F.row_number().over(Window.orderBy(F.col('count').desc())))
df_req_rnk.where("crash_cnt between 5 and 15").select(F.col('VEH_MAKE_ID')).show(10,False)

log('INFO','ANALYSIS 4 COMPLETED')


########## Analysis 5: 
#For all the body styles involved in crashes, 
#mention the top ethnic user group of each unique body style  

print("Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style")
print("Analysis 5 Solution:")

windowDept = Window.partitionBy(F.col('VEH_BODY_STYL_ID')).orderBy(F.col("count").desc())

df_person_with_ethnicity = df_primary_person.select(F.col('CRASH_ID'),F.col('PRSN_ETHNICITY_ID')).distinct()
df_req_units = df_units.select(F.col("VEH_BODY_STYL_ID"),F.col("CRASH_ID"))
join_cond = [df_person_with_ethnicity['crash_id'] == df_req_units['crash_id']]
df_units_with_ethnicity = df_req_units.join(df_person_with_ethnicity, on = join_cond, how='inner').drop(df_req_units['crash_id'])
df_rnked = df_units_with_ethnicity.groupby(F.col('VEH_BODY_STYL_ID'),F.col('PRSN_ETHNICITY_ID')).count().sort(F.col('VEH_BODY_STYL_ID'),F.col('count').desc())
df_req = df_rnked.withColumn("body_type_rnk",F.row_number().over(windowDept))
#df_req.show(100,False)
df_res = df_req.where("body_type_rnk = 1").select(F.col('VEH_BODY_STYL_ID'),F.col('PRSN_ETHNICITY_ID'))
df_res.show(50,False)

log('INFO','ANALYSIS 5 COMPLETED')


########## Analysis 6: 
#Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes 
#with alcohols as the contributing factor to a crash (Use Driver Zip Code)

print("Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)")
print("Analysis 6 Solution:")

df_alc_crashes = df_units.where("VEH_BODY_STYL_ID in ('PASSENGER CAR, 2-DOOR','POLICE CAR/TRUCK','PASSENGER CAR, 4-DOOR','SPORT UTILITY VEHICLE')").where("CONTRIB_FACTR_1_ID in ('UNDER INFLUENCE - ALCOHOL','HAD BEEN DRINKING') or CONTRIB_FACTR_2_ID in ('UNDER INFLUENCE - ALCOHOL','HAD BEEN DRINKING') or CONTRIB_FACTR_P1_ID in ('UNDER INFLUENCE - ALCOHOL','HAD BEEN DRINKING')").select(F.col("crash_id"),F.col('VEH_BODY_STYL_ID'))
#df_alc_crashes.show(10,False)
df_person_with_zip = df_primary_person.select(F.col("crash_id"),F.col('DRVR_ZIP'))
join_cond = [df_alc_crashes['crash_id'] == df_person_with_zip['crash_id']]
df_req = df_alc_crashes.join(df_person_with_zip, on = join_cond, how='inner').drop(df_person_with_zip['crash_id'])
#df_req.show(15,False)
df_res = df_req.groupby(F.col('DRVR_ZIP')).count()
df_res.sort(F.col('count').desc()).show(5,False)

log('INFO','ANALYSIS 6 COMPLETED')

########## Analysis 7: 
#Count of Distinct Crash IDs where No Damaged Property was observed 
#and Damage Level (VEH_DMAG_SCL~) is above 4 
#and car avails Insurance

print("Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance")
print("Analysis 7 Solution:")

df_damage_cid = df_damages.select(F.col('crash_id')).distinct()
df_unit_dmg_lvl = df_units.where("VEH_DMAG_SCL_1_ID in('DAMAGED 5','DAMAGED 6','DAMAGED 7 HIGHEST') or VEH_DMAG_SCL_2_ID in('DAMAGED 5','DAMAGED 6','DAMAGED 7 HIGHEST')")
df_unit_with_insurance = df_unit_dmg_lvl.where("FIN_RESP_TYPE_ID like '%INSURANCE%'").select(F.col('crash_id'),F.col('VEH_DMAG_SCL_1_ID'),F.col('VEH_DMAG_SCL_2_ID'),F.col('FIN_RESP_TYPE_ID'))
join_cond = [df_unit_with_insurance['crash_id'] == df_damage_cid['crash_id']]
df_req = df_unit_with_insurance.join(df_damage_cid, on = join_cond, how='left_anti').distinct()#.drop(df_person_with_zip['crash_id'])
res7 = df_req.select(F.col('crash_id')).distinct().count()
print(res7)

log('INFO','ANALYSIS 7 COMPLETED')


########## Analysis 8: 
#Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, 
#has licensed Drivers, 
#uses top 10 used vehicle colours 
#and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

print("Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)")
print("Analysis 8 Solution:")

#Getting crashes where charge was speeding related:
df_speeding_charge = df_charges.where("charge like '%SPEED%'").select(F.col('crash_id'),F.col('CHARGE'))

#Getting the top 25 states with highest number of offences:
df_top_state = df_primary_person.groupby(F.col('DRVR_LIC_STATE_ID')).count()
df_rnk_state = df_top_state.withColumn('state_rnk',F.row_number().over(Window.orderBy(F.col('count').desc())))
df_rnk_state_req = df_rnk_state.where("state_rnk <= 25").select(F.col('DRVR_LIC_STATE_ID'))
#df_rnk_state_req.show(10,False)

#Getting Licensed Drivers:
df_lic_drvr = df_primary_person.where("DRVR_LIC_TYPE_ID in ('COMMERCIAL DRIVER LIC.','DRIVER LICENSE')").select(F.col('crash_id'),F.col('DRVR_LIC_STATE_ID'),F.col('DRVR_LIC_TYPE_ID'))

#Getting Licensed Drivers those have car licensed with the Top 25 states with highest number of offences:
df_lic_drvr_top_state = df_lic_drvr.join(df_rnk_state_req, on = [df_lic_drvr['DRVR_LIC_STATE_ID'] == df_rnk_state_req['DRVR_LIC_STATE_ID']],how = 'inner').drop(df_rnk_state_req['DRVR_LIC_STATE_ID'])
#df_lic_drvr_top_state.show(10,False)

#Getting Licensed Drivers from Top states with speeding related charges:
drvr_speed_cond = [df_lic_drvr_top_state['crash_id'] == df_speeding_charge['crash_id']]
df_lic_drvr_speeding = df_lic_drvr_top_state.join(df_speeding_charge, on = drvr_speed_cond, how = 'inner').drop(df_speeding_charge['crash_id'])
#df_lic_drvr_speeding.show(15,False)

#Getting the top 10 used vehicles colours:
df_top_clr_vhcl = df_units.groupby(F.col('VEH_COLOR_ID')).count()
df_rnk_clr = df_top_clr_vhcl.withColumn('clr_rnk',F.row_number().over(Window.orderBy(F.col('count').desc())))
df_rnk_clr_req = df_rnk_clr.where("clr_rnk <= 10").select(F.col('VEH_COLOR_ID'))
#df_rnk_clr_req.show(10,False)

#Getting crash_ids where the colour of car was in top 10 used vehicles colours:
df_units_req = df_units.join(df_rnk_clr_req, on = [df_units['VEH_COLOR_ID'] == df_rnk_clr_req['VEH_COLOR_ID']], how = 'inner').drop(df_rnk_clr_req['VEH_COLOR_ID'])
df_units_top_clr = df_units_req.select(F.col('crash_id'),F.col('VEH_COLOR_ID'),F.col('VEH_MAKE_ID'))
#df_units_top_clr.show(10,False)

#Getting licensed speedy drivers from Top states driving top coloured cars:
drvr_clr_cond = [df_lic_drvr_speeding['crash_id'] == df_units_top_clr['crash_id']]
df_req = df_lic_drvr_speeding.join(df_units_top_clr, on = drvr_clr_cond,how = 'inner')

#Getting the Top Vehicle Makers:
df_veh_req = df_req.groupby(F.col('VEH_MAKE_ID')).count()
df_veh_req_rnk = df_veh_req.withColumn('veh_rnk',F.row_number().over(Window.orderBy(F.col('count').desc())))
df_veh_req_rnk.where("veh_rnk <= 5").select(F.col('VEH_MAKE_ID')).show(10,False)

log('INFO','ANALYSIS 8 COMPLETED')

log('INFO','EXECUTION COMPLETED')

############################################################# xxxx END xxxx #####################################################################################