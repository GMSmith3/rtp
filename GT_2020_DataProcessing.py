#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 9 2020

@author: georgesmith
"""

import pandas as pd
import numpy as np


contacts = pd.read_csv("/Users/georgesmith/Desktop/RTP/Season 28/Development/#GIVINGTUESDAY 2020/AllAccountsandContacts_Jan09_2021.csv", dtype={'Zip': object})
gt_noCID = pd.read_csv("/Users/georgesmith/Desktop/RTP/Season 28/Development/#GIVINGTUESDAY 2020/GivingTuesday_Donation_Data_Final_Jan08_2021_noCID.csv", dtype={'Zip': object})
gt_wCID = pd.read_csv("/Users/georgesmith/Desktop/RTP/Season 28/Development/#GIVINGTUESDAY 2020/GivingTuesday_Donation_Data_Final_Jan08_2021_wCID.csv", dtype={'Zip': object})

#Select necessary GT data columns
gt_noCID = gt_noCID.iloc[:, np.r_[2, 4:10, 11, 13, 23, 30:33]]
gt_wCID = gt_wCID.iloc[:, np.r_[2, 11, 13, 23, 30:33, 43]]

#Rename contacts & gt columns
contact_cols = ['firstName', 'lastName', 'contactID', 'accountID', 'accountName',
       'street', 'city', 'state', 'zip', 'email', 'phone']
contacts.columns = contact_cols

gt_noCID_cols = ['date', 'firstName', 'lastName', 'street', 'city', 'state', 'zip', 'email', 'dedication', 
 'totalAmount', 'coveredCost', 'netCost', 'amount']
gt_wCID_cols = ['date', 'email', 'dedication', 'totalAmount', 'coveredCost', 'netCost', 'amount', 'contactID']
gt_noCID.columns = gt_noCID_cols
gt_wCID.columns = gt_wCID_cols

#Confirm expected renaming and check data types:
contacts.dtypes
gt_noCID.dtypes
gt_wCID.dtypes

#####################################
###Join gt_wCID and contacts dfs to faciliate uploading donation into PatronManager
#####################################
gt_wCID = pd.merge(gt_wCID, contacts, how='left', on=['contactID'], copy=False)

#####################################
###Prep Remaining Data for Optimal Joining
#####################################
#remove +4 from applicable zipcodes (leaving only 1st 5 digits)
def clean_zip(z):#parses zip 5 from 9-digit zips; accepts series of zipcodes
    return z.str[:5]

#imputes nan with blanks, parses zip5 and lowercases emails for all dataframes in the list provided as an argument
def standardize_cols(data_frame_list): 
    for df in data_frame_list:
        df.fillna("", inplace=True)
        df['zip'] = clean_zip(df['zip'])
        df['email'] = df['email'].str.lower()
    return

#Apply function to standardize dataframe columns
dfs=[contacts, gt_noCID]
standardize_cols(dfs)

#Loop through remaining possible join columns to standardize text
jcols = ['firstName', 'lastName', 'street', 'city']
for jcol in jcols:
    contacts[jcol] = (contacts[jcol].str.replace(r'[^\w\s]+', '')).str.lower()
    gt_noCID[jcol] = (gt_noCID[jcol].str.replace(r'[^\w\s]+', '')).str.lower()


#####################################
###Join GivingTuesday results with contacts df to add Account and Contact IDs
#####################################
merged_gt_noCID_justemailkey = pd.merge(gt_noCID, contacts, how='left', on=['email'], copy=False)
merged_gt_noCID_juststreetkey = pd.merge(gt_noCID, contacts, how='left', on=['street'], copy=False)
merged_gt_noCID_nameskey = pd.merge(gt_noCID, contacts, how='left', on=['firstName', 'lastName'], copy=False)
merged_gt_noCID_streetkey = pd.merge(gt_noCID, contacts, how='left', on=['firstName', 'lastName', 'street'], copy=False)
merged_gt_noCID_emailkey = pd.merge(gt_noCID, contacts, how='left', on=['firstName', 'lastName', 'email'], copy=False)
merged_gt_noCID_ezkey = pd.merge(gt_noCID, contacts, how='left', on=['firstName', 'lastName', 'zip', 'email'], copy=False)
merged_gt_noCID_zipkey = pd.merge(gt_noCID, contacts, how='left', on=['firstName', 'lastName', 'zip'], copy=False)
merged_gt_noCID_citykey = pd.merge(gt_noCID, contacts, how='left', on=['firstName', 'lastName', 'city'], copy=False)


#subset rows where contactID is null from merged_gt_noCID_ezkey as it returned only the remaining donor records
cIDAssigned = merged_gt_noCID_ezkey[merged_gt_noCID_ezkey.contactID.notnull()]
toAssign = merged_gt_noCID_ezkey[merged_gt_noCID_ezkey.contactID.isna()].iloc[:, :13]
toAssign.columns = gt_noCID_cols

merged_toAssign_emailkey = pd.merge(toAssign, contacts.drop_duplicates(subset=['firstName', 'lastName', 'email']), how='left', on=['firstName', 'lastName', 'email'], copy=False)
cIDAssigned2 = merged_toAssign_emailkey[merged_toAssign_emailkey.contactID.notnull()]
toAssign2 = merged_toAssign_emailkey[merged_toAssign_emailkey.contactID.isna()].iloc[:, :13]
toAssign2.columns = gt_noCID_cols

merged_toAssign_nameskey = pd.merge(toAssign2, contacts.drop_duplicates(subset=['firstName', 'lastName']), how='left', on=['firstName', 'lastName'], copy=False)
cIDAssigned3 = merged_toAssign_nameskey[merged_toAssign_nameskey.contactID.notnull()]
toAssign3 = merged_toAssign_nameskey[merged_toAssign_nameskey.contactID.isna()].iloc[:, :13]
toAssign3.columns = gt_noCID_cols

merged_toAssign_onlyemailkey = pd.merge(toAssign3, contacts.drop_duplicates(subset=['email']), how='left', on=['email'], copy=False)
cIDAssigned4 = merged_toAssign_onlyemailkey[merged_toAssign_onlyemailkey.contactID.notnull()]
toAssign4 = merged_toAssign_onlyemailkey[merged_toAssign_onlyemailkey.contactID.isna()].iloc[:, :13]
toAssign4.columns = gt_noCID_cols

#Select (and rename as necessary) common columns for the cIDAssigned dataframes + then combine them
gt_wCID = gt_wCID.loc[:, ['date', 'firstName', 'lastName', 'street', 'city', 'state', 'zip',
       'email_y', 'dedication', 'totalAmount', 'coveredCost', 'netCost',
       'amount', 'contactID', 'accountID', 'accountName']]

cIDAssigned = cIDAssigned.loc[:, ['date', 'firstName', 'lastName', 'street_y', 'city_y', 'state_y', 'zip',
       'email', 'dedication', 'totalAmount', 'coveredCost', 'netCost',
       'amount', 'contactID', 'accountID', 'accountName']]

cIDAssigned2 = cIDAssigned2.loc[:, ['date', 'firstName', 'lastName', 'street_y', 'city_y', 'state_y', 'zip_y',
       'email', 'dedication', 'totalAmount', 'coveredCost', 'netCost',
       'amount', 'contactID', 'accountID', 'accountName']]

cIDAssigned3 = cIDAssigned3.loc[:, ['date', 'firstName', 'lastName', 'street_y', 'city_y', 'state_y', 'zip_y',
       'email_y', 'dedication', 'totalAmount', 'coveredCost', 'netCost',
       'amount', 'contactID', 'accountID', 'accountName']]

cIDAssigned4 = cIDAssigned4.loc[:, ['date', 'firstName_y', 'lastName_y', 'street_y', 'city_y', 'state_y', 'zip_y',
       'email', 'dedication', 'totalAmount', 'coveredCost', 'netCost',
       'amount', 'contactID', 'accountID', 'accountName']]

gt_wCID.columns = ['date', 'firstName', 'lastName', 'street', 'city', 'state', 'zip',
       'email', 'dedication', 'totalAmount', 'coveredCost', 'netCost', 'amount', 'contactID', 'accountID', 'accountName']
cIDAssigned.columns = ['date', 'firstName', 'lastName', 'street', 'city', 'state', 'zip',
       'email', 'dedication', 'totalAmount', 'coveredCost', 'netCost', 'amount', 'contactID', 'accountID', 'accountName']
cIDAssigned2.columns = ['date', 'firstName', 'lastName', 'street', 'city', 'state', 'zip',
       'email', 'dedication', 'totalAmount', 'coveredCost', 'netCost', 'amount', 'contactID', 'accountID', 'accountName']
cIDAssigned3.columns = ['date', 'firstName', 'lastName', 'street', 'city', 'state', 'zip',
       'email', 'dedication', 'totalAmount', 'coveredCost', 'netCost', 'amount', 'contactID', 'accountID', 'accountName']
cIDAssigned4.columns = ['date', 'firstName', 'lastName', 'street', 'city', 'state', 'zip',
       'email', 'dedication', 'totalAmount', 'coveredCost', 'netCost', 'amount', 'contactID', 'accountID', 'accountName']

#Combine the dataframes and export the new df for uploading to PM
gtDonations_4Upload = pd.concat([gt_wCID, cIDAssigned, cIDAssigned2, cIDAssigned3, cIDAssigned4])

#Save to csv:
gtDonations_4Upload.to_csv("/Users/georgesmith/Desktop/RTP/Season 28/Development/#GIVINGTUESDAY 2020/GT2020_donation_records_import.csv")

