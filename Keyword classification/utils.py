import pandas as pd, numpy as np
from io import StringIO
import os
from google.cloud import storage
from config import *
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats,integrate
import math 
from pandas.compat._optional import import_optional_dependency
from pandasql import sqldf
pandas_gbq = import_optional_dependency("pandas_gbq", extra="")

#read data from parameter table and created word dictionary and word list
def get_parameter_dic():
    query_str = ''' SELECT * FROM `%s.%s`'''%(DATASET_NAME,PARAMETER_TABLE_NAME)
    par_facebook = pandas_gbq.read_gbq(query_str)
    dict_ad_name_brand_info = {}
    dict_product_category_abbreviation = {}
    dict_campain_name_campain_type_info = {}
    dict_audience_group_abbreviation = {}
    dict_targeting_acronym = {}
    dict_optimization_acronym ={}
    dict_creative_format_abbreviation = {}
    dict_content_group_abbreviation = {}
    dict_e_com_adname ={}
    dict_channel = {}
    dict_aw_obj = {}
    dict_shop_name_shp = {}
    dict_seller_name_lzd = {}
    dict_fb_account = {}

    list_ad_name_brand_info = []
    list_product_category_abbreviation = []
    list_campain_name_campain_type_info = []
    list_audience_group_abbreviation = []
    list_targeting_acronym = []
    list_optimization_acronym =[]
    list_creative_format_abbreviation = []
    list_content_group_abbreviation = []
    list_e_com_adname =[]
    list_channel = []
    list_aw_obj = []
    list_shop_name_shp = []
    list_seller_name_lzd = []
    list_fb_account = []

    for j in range(0,len(par_facebook)):
    
        if type(par_facebook["aw_obj"][j]) == str:
            dict_aw_obj[par_facebook["aw_obj"][j]] = par_facebook["awareness_objective"][j]
            dict_aw_obj[par_facebook["awareness_objective"][j]] = par_facebook["awareness_objective"][j]
            list_aw_obj.append(par_facebook["aw_obj"][j])

        if type(par_facebook["shop_name_shp"][j]) == str:
            temp_dict = {}
            temp_dict['division'] = str(par_facebook["division_shp"][j])
            temp_dict['total_brand'] = par_facebook["total_brand_shp"][j]
            dict_shop_name_shp[par_facebook["shop_name_shp"][j]] = temp_dict
            list_shop_name_shp.append(par_facebook["shop_name_shp"][j])
            
        if type(par_facebook["seller_name_lzd"][j]) == str:
            temp_dict = {}
            temp_dict['division'] = str(par_facebook["division_lzd"][j])
            temp_dict['total_brand'] = par_facebook["total_brand_lzd"][j]
            dict_seller_name_lzd[par_facebook["seller_name_lzd"][j]] = temp_dict
            list_seller_name_lzd.append(par_facebook["seller_name_lzd"][j])
            
        if type(par_facebook["fb_account"][j]) == str:
            temp_dict = {}
            temp_dict['division'] = str(par_facebook["division_fb"][j])
            temp_dict['total_brand'] = par_facebook["total_brand_fb"][j]
            dict_fb_account[par_facebook["fb_account"][j]] = temp_dict
            list_fb_account.append(par_facebook["fb_account"][j])
            
        if type(par_facebook["ad_name_brand_info"][j]) == str:
            temp_dict = {}
            temp_dict['division'] = str(par_facebook["division"][j])
            temp_dict['total_brand'] = par_facebook["total_brand"][j]
            temp_dict['brand_name'] = par_facebook["brand_name"][j]
            #dict[par_facebook["ad_name_brand_info"][j]] = temp_dict
            dict_ad_name_brand_info[par_facebook["ad_name_brand_info"][j]] = temp_dict
            list_ad_name_brand_info.append(par_facebook["ad_name_brand_info"][j])    
            

        if type(par_facebook["product_category_abbreviation"][j]) == str:
            #dict[par_facebook["product_category_abbreviation"][j]] = par_facebook["product_category_detail"][j]
            dict_product_category_abbreviation[par_facebook["product_category_abbreviation"][j]] = par_facebook["product_category_detail"][j]
            dict_product_category_abbreviation[par_facebook["product_category_detail"][j]] = par_facebook["product_category_detail"][j]
            list_product_category_abbreviation.append(par_facebook["product_category_abbreviation"][j])
        
        if type(par_facebook["campain_name_campain_type_info"][j]) == str:
            #dict[par_facebook["campain_name_campain_type_info"][j]] = par_facebook["campaign"][j]
            dict_campain_name_campain_type_info[par_facebook["campain_name_campain_type_info"][j]] = par_facebook["campaign"][j]
            dict_campain_name_campain_type_info[par_facebook["campaign"][j]] = par_facebook["campaign"][j]
            list_campain_name_campain_type_info.append(par_facebook["campain_name_campain_type_info"][j])
        
        if type(par_facebook["audience_group_abbreviation"][j]) == str:
            #dict[par_facebook["audience_group_abbreviation"][j]] = par_facebook["audience_group"][j]
            dict_audience_group_abbreviation[par_facebook["audience_group_abbreviation"][j]] = par_facebook["audience_group"][j]
            dict_audience_group_abbreviation[par_facebook["audience_group"][j]] = par_facebook["audience_group"][j]
            list_audience_group_abbreviation.append(par_facebook["audience_group_abbreviation"][j])
        
        if type(par_facebook["targeting_acronym"][j]) == str:
            #dict[par_facebook["targeting_acronym"][j]] = par_facebook["targeting"][j]
            dict_targeting_acronym[par_facebook["targeting_acronym"][j]] = par_facebook["targeting"][j]
            dict_targeting_acronym[par_facebook["targeting"][j]] = par_facebook["targeting"][j]
            list_targeting_acronym.append(par_facebook["targeting_acronym"][j])

        if type(par_facebook["optimization_acronym"][j]) == str:
            #dict[par_facebook["optimization_acronym"][j]] = par_facebook["optimization"][j]
            dict_optimization_acronym[par_facebook["optimization_acronym"][j]] = par_facebook["optimization"][j]
            dict_optimization_acronym[par_facebook["optimization"][j]] = par_facebook["optimization"][j]
            list_optimization_acronym.append(par_facebook["optimization_acronym"][j])

        if type(par_facebook["creative_format_abbreviation"][j]) == str:
            #dict[par_facebook["creative_format_abbreviation"][j]] = par_facebook["creative_format"][j]
            dict_creative_format_abbreviation[par_facebook["creative_format_abbreviation"][j]] = par_facebook["creative_format"][j]
            dict_creative_format_abbreviation[par_facebook["creative_format"][j]] = par_facebook["creative_format"][j]
            list_creative_format_abbreviation.append(par_facebook["creative_format_abbreviation"][j])
        
        if type(par_facebook["content_group_abbreviation"][j]) == str:
            #dict[par_facebook["content_group_abbreviation"][j]] = par_facebook["content_group"][j]
            dict_content_group_abbreviation[par_facebook["content_group_abbreviation"][j]] = par_facebook["content_group"][j]
            dict_content_group_abbreviation[par_facebook["content_group"][j]] = par_facebook["content_group"][j]
            list_content_group_abbreviation.append(par_facebook["content_group_abbreviation"][j])
        
        if type(par_facebook["e_com_adname"][j]) == str:
            #dict[par_facebook["platform_p"][j]] = par_facebook["ecom_platform"][j]
            dict_e_com_adname[par_facebook["e_com_adname"][j]] = par_facebook["e_com_platform"][j]
            dict_e_com_adname[par_facebook["e_com_platform"][j]] = par_facebook["e_com_platform"][j]
            list_e_com_adname.append(par_facebook["e_com_adname"][j])
        
        if type(par_facebook["channel"][j]) == str:
            dict_channel[par_facebook["channel"][j]] = par_facebook["channel_name"][j] 
            dict_channel[par_facebook["channel_name"][j]] = par_facebook["channel_name"][j]    
            list_channel.append(par_facebook["channel"][j])

    return dict_ad_name_brand_info,dict_product_category_abbreviation,dict_campain_name_campain_type_info,dict_audience_group_abbreviation,dict_targeting_acronym,dict_optimization_acronym,dict_creative_format_abbreviation,dict_content_group_abbreviation,dict_e_com_adname,dict_channel,dict_aw_obj,dict_shop_name_shp,dict_seller_name_lzd,dict_fb_account,list_ad_name_brand_info,list_product_category_abbreviation,list_campain_name_campain_type_info,list_audience_group_abbreviation,list_targeting_acronym,list_optimization_acronym,list_creative_format_abbreviation,list_content_group_abbreviation,list_e_com_adname,list_channel,list_aw_obj,list_shop_name_shp,list_seller_name_lzd,list_fb_account




def format_column_name(df):
    df.columns = df.columns.map(lambda x: x.lower().
                                replace('(dd/mm/yyyy)','').
                                replace('-','_').                
                                replace(' ','_').
                                replace('(','_').
                                replace('/','_or_').
                                replace('₫','vnd').
                                replace('%','').
                                replace(')','').
                                replace(':','_').
                                replace('__','_'))
    return df


#convert all the columns type to string
def format_columns_type(df):
    column_list=df.columns
    for i in range(0,len(column_list)):
        keyname = column_list[i]
        df[keyname] = df[keyname].astype("str")
    return df





def download_files_df(download_url,isResultList, sheet_name = 0,header = 0,types = {}):
    file_list = []
    final_df = pd.DataFrame()
    client = storage.Client()
    bucket = storage.Bucket(client, BUCKET_NAME, user_project=PROJECT_NAME)
    all_blobs = list(client.list_blobs(bucket))
    for i in range(0,len(all_blobs)):
        path_str = all_blobs[i].path
        format_path_str = path_str.replace('%2F','/')
        #path_str = path_str.replace('%2F','/')
        if format_path_str.find(download_url)>0:
            path = path_str.split('/')[-1].replace('%2F','/').replace('%20',' ')
            file_name = path.split('/')[-1]
            if len(file_name)>0:
                blob = storage.Blob(path, bucket)
                contents_bytes = blob.download_as_bytes()
                # s=str(contents_bytes)
                # data = StringIO(s)
                
                if path.find("xlsx")>0:
                    df = pd.read_excel(contents_bytes, sheet_name = sheet_name,header=header, engine='openpyxl', dtype = types)
                    if final_df.shape[1]==0:
                        final_df=df
                    else:
                        final_df=final_df.append(df)
                if path.find("csv")>0:
                    df = pd.read_csv(contents_bytes)
                    if final_df.shape[1]==0:
                        final_df=df
                    else:
                        final_df=final_df.append(df)
    final_df.columns = final_df.columns.map(lambda x: x.lower().
                                replace('(dd/mm/yyyy)','').
                                replace('-','_').                
                                replace(' ','_').
                                replace(',','_').
                                replace('(','_').
                                replace('.','').
                                replace('/','_or_').
                                replace('₫','vnd').
                                replace('%','').
                                replace(')','').
                                replace(':','_').
                                replace('__','_'))
    final_df = final_df.dropna(how = 'all')
    
    return final_df



#download files from gcs
def download_files(download_url,isResultList, sheet_name = 0,header = 0,types = {}):
    if not os.path.exists('./tmpfiles'):
        os.makedirs('./tmpfiles')
    file_list = []

    client = storage.Client()
    bucket = storage.Bucket(client, BUCKET_NAME, user_project=PROJECT_NAME)
    all_blobs = list(client.list_blobs(bucket))
    for i in range(0,len(all_blobs)):
        ####
        #all_blobs[i].path.replace('%20',' ').replace('%28','(').replace('%29',')')
        ###
        path_str = all_blobs[i].path
        format_path_str = path_str.replace('%2F','/')
        #path_str = path_str.replace('%2F','/')
        if format_path_str.find(download_url)>0:
            path = path_str.split('/')[-1].replace('%2F','/')
            file_name = path.split('/')[-1]#.replace('%20',' ').replace('%28','(').replace('%29',')')
            #file_name = file_name.replace('%','')
            if len(file_name)>0:
                blob = storage.Blob(path, bucket)
                with open('./tmpfiles/'+file_name, 'wb') as file_obj:
                    client.download_blob_to_file(blob, file_obj)
                    file_list.append('./tmpfiles/'+file_name)
                    
    if isResultList:
        return file_list
                    
    final_df = pd.DataFrame()
    for i in range(0,len(file_list)):
        #print(final_df.shape)
        path = file_list[i]
        if path.find("xlsx")>0:  
            df = pd.read_excel(path, sheet_name = sheet_name,header=header, engine='openpyxl', dtype = types)
            # if types!=None:
            #     df = pd.read_excel(path, sheet_name = sheet_name,header=header, engine='openpyxl', dtype = types)
            # else:
            #     df = pd.read_excel(path, sheet_name = sheet_name,header=header, engine='openpyxl')
            os.remove(path)
            if final_df.shape[1]==0:
                final_df=df
            else:
                final_df=final_df.append(df)
        if path.find("csv")>0:
            df = pd.read_csv(path)
            os.remove(path)
            if final_df.shape[1]==0:
                final_df=df
            else:
                final_df=final_df.append(df) 
                
    final_df.columns = final_df.columns.map(lambda x: x.lower().
                                replace('(dd/mm/yyyy)','').
                                replace('-','_').                
                                replace(' ','_').
                                replace(',','_').
                                replace('(','_').
                                replace('.','').
                                replace('/','_or_').
                                replace('₫','vnd').
                                replace('%','').
                                replace(')','').
                                replace(':','_').
                                replace('__','_'))
    final_df = final_df.dropna(how = 'all')
    
    return final_df



#fuzzymatch token sort ratio
def findMatchKey(word,keywordList):
    return process.extract(word, keywordList, scorer=fuzz.token_sort_ratio, limit=1)

#fuzzymatch token set ratio
def findMatchKey2(word,keywordList):
    return process.extract(word, keywordList, scorer=fuzz.token_set_ratio, limit=1)

#fuzzymatch token sort ratio
def findMatchKey3(word,keywordList):
    return process.extract(word, keywordList, scorer=fuzz.ratio, limit=1)

#fuzzymatch with match rate limit
def fuzzyMatch(word,keywordList,matchRate = 90):
    if word is None or len(word)==0:
        return [tuple(['undefined',100])] 
    
    resultList = process.extract(word, keywordList, scorer=fuzz.token_sort_ratio, limit=1)
    if resultList[0][1]<matchRate:
        resultList[0] = tuple([word,100])
    return  resultList


#fuzzymatch and record match score+doing analyze
class fuzzyMatchTool():
 
    def __init__(self, match_rate=DEFAULT_MATCH_RATE):
 
        self.match_rate_list = []
        self.match_rate = match_rate
        self.match_df = pd.DataFrame(columns = ['key_word','match_word','match_rate'])
        self.match_df_freequency = pd.DataFrame(columns = ['key_word','match_word','match_rate','freequency'])
 
    def fuzzy_match(self,word,keywordList):
        if word is None or len(word)==0:
            return [tuple(['undefined',100])] 
        if len(keywordList) == 0:
            return [tuple(['undefined',100])]
        
        resultList = process.extract(word, keywordList, scorer=fuzz.token_sort_ratio, limit=1)
        self.match_rate_list.append(resultList[0][1])        
        self.match_df.loc[self.match_df.shape[0]] = (word,resultList[0][0],resultList[0][1])        
        if resultList[0][1]<self.match_rate:
            resultList[0] = tuple([word,100])
        return resultList
    
    def match_rate_analyze(self,rate_list = [90,85,80,75]):
        tmp_table = self.match_df       
        sql = 'select t.key_word,t.match_word,avg(t.match_rate) as match_rate,count(*) freequency from tmp_table t group by t.key_word,t.match_word order by match_rate desc,freequency desc'         
        self.match_df_freequency = sqldf(sql) 

        sns.displot(self.match_df, x="match_rate", kde=True)          
        #sns.jointplot(data=self.match_df_freequency, y="freequency", x="match_rate")
        sns.displot(self.match_df_freequency, y="freequency", x="match_rate", kind="kde")

        for i in rate_list:
            print("########match rate lower limit:%d, cover rate: %.2f"%(i,(self.match_df_freequency[self.match_df_freequency['match_rate'] >=i].freequency.sum()/self.match_df_freequency.freequency.sum())))
            print("Unmatched example:")
            print(self.match_df_freequency[self.match_df_freequency['match_rate'] <i].head(5))
            
        return self.match_df_freequency

 