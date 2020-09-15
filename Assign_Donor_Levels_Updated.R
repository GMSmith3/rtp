#Set working directory
setwd("/Users/georgesmith/Desktop/RTP/Season 27/Stewardship Program")
#Check working directory
getwd()
#Load the packages I know I'll need:
library(readr)
library(data.table)
library(dplyr)
library(writexl) #ended up needing this package to write files to excel with write_xlsx()
#Read in the data using the fread function of data.table (this function automatically reads in the 
#date in the correct format as a string) & creates a data table:
Data <- fread("Donors_July012018_Mar072020.csv")
#Convert CloseDate to date data type
Data$CloseDate<-as.Date(Data$CloseDate, format = "%m/%d/%y")
#r convert appropriate columns to factor
Data[,c(2:4,9,15:17,19:22)] <- lapply((Data[,c(2:4,9,15:17,19:22)]), function(x) {
  as.factor(x)
  } )
  
lapply((Data[,c(2:4,9,15:17,19:22)]), function(x) {
  levels(x)
} )

lapply((Data[,c(2:4,9,15:17,19:22)]), function(x) {
  class(x)
} )
#Select only the records where Fund = Annual Fund or is.na:
Data <- Data[Fund =="Annual Fund" | Fund == ""]
#Find Pledge Payments:
pledgepayments <- Data[DonationRecordType == "Pledge Payment",]
#Find pledges:
pledges <- Data[DonationRecordType == "Pledge",]
#Filter out rows based on values in specific columns to get subset on which I'm ready to calculate donation metrics
#and donor stewardship levels.
donors <- Data[((AccountRecordType %in% c("Household", "Individual")) &
    (DonationRecordType %in% c("Donation", "PatronTicket Donation", "Matching", "Pledge Payment")) &
    (Stage != "Refunded") &
    (PaymentType != "Ticket Order Refund")),]
#Filter for giving period (if not imported that way):
donors <- donors[CloseDate >= "2018-07-01" & CloseDate <= "2020-03-07"]
#Calculate the sum of Amount column for every group in AccountID column - (remove NAs from the calculations). Call the new data - DonationTotalsDT:
DonationTotalsDT <- donors[,.(AccountName, InformalSalutation, MailingStreet, MailingCity, MailingState, MailingZip, DonationTotal = sum(Amount, na.rm = TRUE), Amount, PledgeAmount, PaymentSchedule, CloseDate, Fund, PaymentType, Type),by=AccountID]
View(DonationTotalsDT)
#Let's sort the list by AccountID to see how prevalent duplicate records are:
SortByAcct <- DonationTotalsDT[order(AccountID),]
#Remove the duplicate records from DonationTotalsDT based on Account ID (keep most recent row - with most recent donation):
UniqueDT <- unique(DonationTotalsDT[order(CloseDate)], by="AccountID", fromLast = TRUE)
#Let's filter the data table to include only records with DonationTotal >= 125:
DonationTotal125DT <- UniqueDT[DonationTotal >= 125,]
#View(DonationTotal125DT)
#Let's sort the data table in ascending order:
Sorted <- DonationTotal125DT[order(DonationTotal),]
#View(Sorted)
#Show me the records with donation totals between 125 and 249:
Supporters <- Sorted[DonationTotal<250,]
#View(Supporters)
#Show me the records with donation totals between 250 and 499:
Investors <- Sorted[DonationTotal>=250 & DonationTotal<500,]
#View(Investors)
#Show me the records with donation totals between 500 and 999:
Underwriters <- Sorted[DonationTotal>=500 & DonationTotal<1000,]
#View(Underwriters)
#Show me the records with donation totals between 1000 and 2499:
Performers <- Sorted[DonationTotal>=1000 & DonationTotal<2500,] 
#View(Performers)
#Show me the records with donation totals between 2500 and 4999:
Directors <- Sorted[DonationTotal>=2500 & DonationTotal<5000,]
#View(Directors)
#Show me the records with donation totals 5000 and up:
Producers <- Sorted[DonationTotal>=5000,]
#View(Producers)
#Alphabetize donors by last name within each group (for playbill listing):
library(stringr)
Supporters <- Supporters %>% 
  arrange(str_extract(AccountName, '\\s.*$'))
Investors <- Investors %>% 
  arrange(str_extract(AccountName, '\\s.*$'))
Underwriters <- Underwriters %>% 
  arrange(str_extract(AccountName, '\\s.*$'))
Performers <- Performers %>% 
  arrange(str_extract(AccountName, '\\s.*$'))
Directors <- Directors %>% 
  arrange(str_extract(AccountName, '\\s.*$'))
Producers <- Producers %>% 
  arrange(str_extract(AccountName, '\\s.*$'))
#Create data table that includes all stewardship program donors = coupon recipients:
SIOW_Donor_List <- rbind(Supporters, Investors, Underwriters, Performers, Directors, Producers)
#Write the coupon recipient file to excel for dissemination:
write_xlsx(SIOW_Donor_List, "/Users/georgesmith/Desktop/RTP/Season 27/SIOW_List_thruJan14_2020.xlsx", col_names = TRUE, format_headers = TRUE)
#Find the donors who donated since 2019-11-13:
NewSuppDonation <- Supporters[CloseDate >= "2019-11-13",]
SortNewSupp <- NewSuppDonation[order(CloseDate),]
NewPerfDonation <- Performers[CloseDate >= "2019-11-13",]
SortNewPerf <- NewPerfDonation[order(CloseDate),]
NewUndDonation <- Underwriters[CloseDate >= "2019-11-13",]
SortNewUnd <- NewUndDonation[order(CloseDate),]
NewInvDonation <- Investors[CloseDate >= "2019-11-13",]
SortNewInv <- NewInvDonation[order(CloseDate),]
NewDirDonation <- Directors[CloseDate >= "2019-11-13",]
SortNewDir <- NewDirDonation[order(CloseDate),]
NewProdDonation <- Producers[CloseDate >= "2019-11-13",]
SortNewProd <- NewProdDonation[order(CloseDate),]
#Find the donors who leveled up since 2019-11-13:
NewSupporters <- SortNewSupp[((DonationTotal - Amount) < 125),]
NewInvestors <- SortNewInv[((DonationTotal - Amount) < 250),]
NewUnderwriters <- SortNewUnd[((DonationTotal - Amount) < 500),]
NewPerformers <- SortNewPerf[((DonationTotal - Amount) < 1000),]
NewDirectors <- SortNewDir[((DonationTotal - Amount) < 2500),]
NewProducers <- SortNewProd[((DonationTotal - Amount) < 5000),]
#Create data table for coupon fulfillment #2 (including all donors who have reached at least 125 since 2019-11-13):
Coupon_Recipients_FF2 <- DonationTotal125DT[CloseDate >= "2019-11-13" & ((DonationTotal - Amount) < 125),]
#Write to excel:
write_xlsx(Coupon_Recipients_FF2, "/Users/georgesmith/Desktop/RTP/Season 27/Stewardship Program/coupons/Stewardship_Coupon_Recipients_FF2_thruFeb17_2020.xlsx", col_names = TRUE, format_headers = TRUE)
#Create a data table that includes all new investors, new underwriters, new performers, all directors and all producers ...
#This data table is the invitation list for Sneak Peek #4:
SneakPeek4InvitationList <- rbind(NewInvestors, NewDirectors, NewPerformers, NewUnderwriters, Directors, Producers)
#Write to excel:
write_xlsx(SneakPeek4InvitationList, "/Users/georgesmith/Desktop/RTP/2018-2019 Sneak Peeks/Cocktails with the Cast/SP4_Invitees.xlsx", col_names = TRUE, format_headers = TRUE)
#Create a data table that includes all performers, all directors and all producers ... invitation list for Cocktails with the Cast:
CwtCInvitationList <- rbind(Performers, Directors, Producers)
#Create data table that includes all stewardship program donors = coupon recipients:
coupon_recipients <- rbind(Supporters, Investors, Underwriters, Performers, Directors, Producers)
#Write to excel:
write_xlsx(CwtCInvitationList, "/Users/georgesmith/Desktop/RTP/2018-2019 Sneak Peeks/Cocktails with the Cast/CwtCInvitationList.xlsx", col_names = TRUE, format_headers = TRUE)
#Write each data table for the different donor levels to xlxs files:
write.xlsx(Supporters, 'SupportersJune102019.xlsx', gridLines = TRUE)
write.xlsx(Investors, 'InvestorsJune102019.xlsx', gridLines = TRUE)
write.xlsx(Underwriters, 'UnderwritersJune102019.xlsx', gridLines = TRUE)
write.xlsx(Directors, 'DirectorsJune102019.xlsx', gridLines = TRUE)
write.xlsx(Producers, 'ProducerJune102019.xlsx', gridLines = TRUE)
write.xlsx(Performers, 'PerformersJune102019.xlsx', gridLines = TRUE)