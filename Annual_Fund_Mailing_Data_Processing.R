# Set working directory
setwd("/Users/georgesmith/Desktop/RTP/Season 28/Development/Fall Annual Fund 2020")
# Check working directory
getwd()
# Load the packages I know I'll need:
library(readr)
library(data.table)
library(dplyr)
library(writexl)
library(scales)
library(stringr)
library(tidyverse)

# Let's import the datatables (dts) containing AF donors and ticket buyers (7/1/18 - 10/29/20):
afDonors <- fread("AFDonations_Jul2018_Nov182020.csv")
tickets <- fread("Ticket_Buyers_thru_Nov182020.csv")
accounts <- fread("AllAccountsandContacts_Nov18_2020.csv")
subscribersDonateOrCredit <- fread("SubscribersCreditDispensation.csv")
curFall19Salutations <- fread("current_donors_fall2019_salutations.csv")
#Import Give OUT Day data for complete new donor records with necessary fields that are blank or NA
gOUTDay2020 <- fread("GiveOUTDay2020_addrs.csv")
#Import data necessary to determine which accounts' only activity was purchase of tix
#to canceled Joan of Arc production
#Import datatable containing only the accounts that purchased tickets to Joan of Arc
joans <- fread("Joan_TicketBuyers.csv")
#Import datatable containing all ticket purchases (by account) EXCEPT Joan of Arc
nonJoans <- fread("Not_Joan_TicketBuyers.csv")

# Create consistent columns names across datatables:
colnames(afDonors) <- c("accountID", "accountRecordType", "doNotMail", "donationRecordType", "informalSalutation",
                        "accountName", "amount", "closeDate", "emailOptOut",  "email", "street", "city", "state",
                        "zip", "fund", "frequency", "paymentType", "pledgeAmount", "paymentSchedule", "type", "stage",
                        "fiscalYear", "lastDonationAmtPM", "lastDonationDatePM", "donorLastFirst")

# Convert colnames in accounts dt to camel case to match those in other dts
colnames(accounts) <- c("firstName", "lastName", "contactID", "accountID", "accountName", "street", 
                        "city", "state", "zip",  "email", "phone", "acctInfSal", "contactInfSal")

# Convert colnames in tickets dt to camel case to match those in other dts
colnames(tickets) <- c("contactID", "accountName", "contactName", "firstName", "lastName", "status", "donationName",
                         "donationAmount", "street", "city", "state", "zip", "subcriptionStatus", "createdDate", "priceLevelName", "tixOrderInfSal")

# Create donor datatable that includes only households and individuals:
afDonors <- afDonors[accountRecordType %in% c("Household", "Individual")]

# Remove rows where amount is na (they mess up later processing)
afDonors <- afDonors[!is.na(amount)]

#Impute lastDonationAmtPM with 0 where it = NA
afDonors$lastDonationAmtPM[is.na(afDonors$lastDonationAmtPM)] <- 0

# Convert date columns to Date data type to allow for filtering on this feature:
afDonors$closeDate <- as.Date(afDonors$closeDate, format = "%m/%d/%y")
afDonors$lastDonationDatePM <- as.Date(afDonors$lastDonationDatePM, format = "%m/%d/%y")
tickets$createdDate <- as.Date(tickets$createdDate, format = "%m/%d/%y")

#Extract firstName from donorLastFirst -- add firstName column
afDonors$firstName <- sapply(strsplit(afDonors$donorLastFirst, ", "), tail, 1)

# #Propercase the accountName column and nearly created firstName column
# afDonors[, accountName := str_to_title(accountName)]
# afDonors[, accountName := str_replace(accountName," And "," and ")]
# afDonors[, firstName := str_to_title(firstName)]


# Append salutation field to afDonors
afDonors <- merge(afDonors, curFall19Salutations, by.x = "accountID", by.y = "accountID", all.x = TRUE)
View(afDonors)

#Create builtSalutation field to serve as default value for records where both salutation and informalSalutation are missing:
afDonors <- cbind(afDonors, data.table(str_match(afDonors$accountName,"(([A-Z][a-z]+\\sand\\s[A-Z][a-z]+)\\s[A-Z]?'?[A-z]+)|(([A-Z][a-z]+)\\s[A-Z][a-z]+\\sand\\s([A-Z][a-z]+)\\s[A-Z][a-z]+)|([A-Z][a-z]+)\\s[A-Z][a-z]+"))[
  ,c("V1", 'V3', "V5", 'V6', 'V7')][, builtSalutation := ifelse(!is.na(V5),
                                                           paste0(V5," and ",V6),
                                                           ifelse(!is.na(V7),
                                                                  paste0(V7),
                                                                  paste0(V3)))][, .(builtSalutation)])

#Add letterSalutation column to this subset(recoding with data.table rather than dplyr):
afDonors[, letterSalutation := fifelse(!(is.na(salutation)),
                                         salutation,
                                         fifelse(informalSalutation != "",
                                                 informalSalutation,
                                                 builtSalutation))]

#Add subscriber column to subscribersDonateOrCredit to allow logic on that field after merge below
subscribersDonateOrCredit$subscriber <- 1

#Append amountDonated and amountCredited fields from subscriber datatable to afDonors:
subscriberCols <- subscribersDonateOrCredit[, 11:14]
afDonors <- merge(afDonors, subscriberCols, by.x = "accountID", by.y = "accountID", all.x = TRUE)

#Change nulls in subscriber column to 0
afDonors$subscriber[is.na(afDonors$subscriber)] <- 0

#Capture refunded donations
refunds <- afDonors[afDonors[, stage == 'Refunded']]
#Remove the donations that were refunded
afDonors <- afDonors[afDonors[, stage != 'Refunded']]

#Determine last stewardship giving level for last period's current donors
lastPeriodGifts <- afDonors[(closeDate > "2018-06-30") & (closeDate < "2020-07-01")][order(accountID, closeDate)]
# Calculate the sum of amount column for every unique accountID. Call the new dataset - lpDonationTot:
lpDonationTot <- lastPeriodGifts[, .(lpDonationTotal = sum(amount, na.rm=TRUE)), by = accountID]
View(lpDonationTot) #merge this with afCurrent below

#Sort lastPeriodGifts by closeDate
lastPeriodGifts <- lastPeriodGifts[order(accountID, closeDate),]

# Remove duplicates from the lastPeriodGifts datatable - keeping the most recent donation - to prepare it to join with lpDonationTot
uniqueLPDonors <- unique(lastPeriodGifts, by="accountID", fromLast = TRUE)
View(uniqueLPDonors)

# Add last period donation totals by joining uniqueLPDonors and lpDonationTot on accountID
lastPeriodDonTotals <- merge(uniqueLPDonors, lpDonationTot, by.x = "accountID", by.y = "accountID", all.x = TRUE)
View(lastPeriodDonTotals)

#Break out donations from afDonors where closeDate < 07/01/2019, separating current from lapsed donations:
#afDonors <- data.table(afDonors) #reverting back to datatable to enable following filtering methods
afLapsed <- afDonors[closeDate < "2019-07-01"][order(accountID, closeDate)] # sorting simplifies selecting latest donation row for each accountID later
afCurrent <- afDonors[closeDate > "2019-06-30"][order(accountID, closeDate)] # sorting simplifies selecting latest donation row for each accountID later

#Create dt with info on last Gift by current donor
lastGiftByCurDonor <- afCurrent[
  order(-closeDate), head(.SD, 1), by = accountID][, 1:8][, .(accountID, lastGiftDate = closeDate, lastGiftAmt = amount)]

# Calculate the sum of amount column for every unique accountID. Call the new dataset - donationTotalDT:
# Add na.rm = TRUE to the sum function to avoid sum being reported as NA for account records with an NA donation amount
afDonorTotals <- afCurrent[, .(donationTotal = sum(amount, na.rm=TRUE)), by = accountID]
View(afDonorTotals)

#Create columns needed for the various donation types and amounts for letter variable data drop-ins
#Add count columns for donations to different categories by accountID:
afCurrent[(type == '')&
            (paymentType == 'Ticket Order Refund')&
            (subscriber == 1)&
            (closeDate > '2020-03-07')&
            (closeDate < '2020-07-01')&
            (!is.na(amountDonated))&
            (amountDonated != ""),
          dsrCounts := .N, by='accountID']
afCurrent[(type == '')&
            (paymentType == 'Ticket Order Refund')&
            (subscriber == 0)&
            (closeDate > '2020-03-07')&
            (closeDate < '2020-07-01'),
          dtrCounts := .N, by='accountID']
afCurrent[(type == 'Give OUT Day'),
          godCounts := .N, by='accountID']
afCurrent[(type == 'GIVINGTUESDAYNOW'),
          gtnCounts := .N, by='accountID']
afCurrent[!(type %in% c('GIVINGTUESDAYNOW', 'Give OUT Day'))&
            (closeDate > '2020-06-05')&
            (closeDate < '2020-08-01'),
          saCounts := .N, by='accountID']
afCurrent[!(type %in% c('GIVINGTUESDAYNOW', 'Give OUT Day'))&
            (paymentType != 'Ticket Order Refund')&
            (is.na(saCounts))&
            (closeDate > '2020-03-08')&
            (closeDate < '2020-11-10'),
          rgCounts := .N, by='accountID']
afCurrent[!(type %in% c('GIVINGTUESDAYNOW', 'Give OUT Day'))&
            (paymentType == 'Ticket Order Refund')&(closeDate > '2020-08-01'),
          rRefundCount := .N, by='accountID']

#Use counts above to ensure grammatical agreement with variable drop-ins
#Created donatedSubRefund and donatedSubRefundAmt
afCurrent[(type == '')&
             (paymentType == 'Ticket Order Refund')&
             (subscriber == 1)&
            (closeDate > '2020-03-07')&
            (closeDate < '2020-07-01')&
            (!is.na(amountDonated))&
            (amountDonated != ""),
          donatedSubRefund := fifelse(dsrCounts == 1,
                                      'donating your season 27 subscription refund back to RTP',
                                      'donating your season 27 subscription refunds back to RTP')]

afCurrent[(type == '')&
            (paymentType == 'Ticket Order Refund')&
            (subscriber == 1)&
            (closeDate > '2020-03-07')&
            (closeDate < '2020-07-01')&
            (!is.na(amountDonated))&
            (amountDonated != ""),
          donatedSubRefundAmt := amount]

#Created donatedTixRefund and donatedTixRefundAmt
afCurrent[(type == '')&
            (paymentType == 'Ticket Order Refund')&
            (subscriber == 0)&
            (closeDate > '2020-03-07')&
            (closeDate < '2020-07-01'),
          donatedTixRefund := fifelse(dtrCounts == 1,
                                      "donating last season's ticket refund back to RTP.",
                                      "donating last season's ticket refunds back to RTP.")]
afCurrent[(type == '')&
            (paymentType == 'Ticket Order Refund')&
            (subscriber == 0)&
            (closeDate > '2020-03-07')&
            (closeDate < '2020-07-01'),
          donatedTixRefundAmt := amount]

#gtn and gtnAmt
afCurrent[(type == 'GIVINGTUESDAYNOW'),
          `:=`(gtn = 'helping us win a major prize in the GIVINGTUESDAYNOW matching challenge',
               gtnAmt = amount)]

#god and godAmt
afCurrent[(type == 'Give OUT Day'),
          `:=`(god = 'contributing to our Give OUT Day initiative',
               godAmt = amount)]
#springAppeal and springAppealAmt
afCurrent[!(type %in% c('GIVINGTUESDAYNOW', 'Give OUT Day'))&
            (closeDate > '2020-06-05')&
            (closeDate < '2020-08-01'),
          `:=`(springAppeal = 'answering our calls to support the spring annual fund campaign',
               springAppealAmt = amount)]
#recentGift and rgAmt
afCurrent[!(type %in% c('GIVINGTUESDAYNOW', 'Give OUT Day'))&
            (paymentType != 'Ticket Order Refund')&
            (is.na(springAppeal))&
            (closeDate > '2020-03-08')&
            (closeDate < '2020-11-10'),
          recentGift := fifelse(rgCounts == 1,
                                'your critical pandemic-era gift',
                                'your critical pandemic-era gifts')]

afCurrent[!(type %in% c('GIVINGTUESDAYNOW', 'Give OUT Day'))&
            (paymentType != 'Ticket Order Refund')&
            (is.na(springAppeal))&
            (closeDate > '2020-03-08')&
            (closeDate < '2020-11-10'),
          rgAmt := amount]

#recentRefundGift and rrgAmt
afCurrent[!(type %in% c('GIVINGTUESDAYNOW', 'Give OUT Day'))&
            (paymentType == 'Ticket Order Refund')&
            (closeDate > '2020-08-01'),
          recentRefundGift := fifelse(rRefundCount == 1,
                                      'reinvesting your recent ticket refund',
                                      'reinvesting your recent ticket refunds',)]
afCurrent[!(type %in% c('GIVINGTUESDAYNOW', 'Give OUT Day'))&
            (paymentType == 'Ticket Order Refund')&
            (closeDate > '2020-08-01'),
          rrgAmt := amount]

###########################
#####Consolidate all recent gifts on a single row -- corresponding to accountID
#############################
gifts <- afCurrent[, .(accountID, accountName, closeDate, letterSalutation, lastDonationDatePM, lastDonationAmtPM, amount, gtn, gtnCounts, god, godCounts, springAppeal, saCounts, recentGift, rgCounts, recentRefundGift, rRefundCount, donatedSubRefund, dsrCounts, donatedTixRefund, dtrCounts)]

giftsCombined <- gifts %>% 
  group_by(accountID) %>% 
  fill(gtn, gtnCounts, god, godCounts, springAppeal, saCounts, recentGift, rgCounts, recentRefundGift, rRefundCount, donatedSubRefund, dsrCounts, donatedTixRefund, dtrCounts, .direction = "downup")

#Sort afCurrent by accountID and closeDate(ascending)
giftsCombined <- data.table(giftsCombined)[order(accountID, closeDate),]

# Remove duplicates from the afCurrent datatable - keeping the most recent donation row - to prepare it to join with afDonorTotals
giftsByAcct <- unique(giftsCombined, by="accountID", fromLast = TRUE)[,wordDate := format(closeDate, format = "%B %d, %Y")]

#Which accounts donated their refunds back
allSubRefGifts <- giftsByAcct[!is.na(donatedSubRefund),] #108 records
allTixRefGifts <- giftsByAcct[!is.na(donatedTixRefund),] #8 records

#Create tables of donors whose last gift was their sub/tix refund
lastSubGiftWasRefund <- giftsByAcct[!is.na(donatedSubRefund)&
                                      (lastDonationDatePM == closeDate)& 
                                      is.na(gtnCounts)&
                                      is.na(godCounts)&
                                      is.na(saCounts)&
                                      is.na(rgCounts)&
                                      is.na(rRefundCount),] #51 records
  
lastTixGiftWasRefund <- giftsByAcct[!is.na(donatedTixRefund)&
                                      (lastDonationDatePM == closeDate)&
                                      is.na(gtnCounts)&
                                      is.na(godCounts)&
                                      is.na(saCounts)&
                                      is.na(rgCounts)&
                                      is.na(rRefundCount),] #5 records 

#52 donors' last donation was their season 27 subscription refund & 5 unique donors' last don was s27 ticket refund 
#Those will be the only donors we recognize again for those gifts.

#Add prevGiftsSentence column for these subs refund donation records - reflecting this as only donation recognition
lastSubGiftWasRefund[, prevGiftsSentence :=  fifelse(dsrCounts == 1,
                                            "kindly donating your Season 27 subscription refund back to RTP.",
                                            "kindly donating your Season 27 subscription refunds back to RTP.")]
#Add prevGiftsSentence column for these ticket refund donation records - reflecting this as only donation recognition
lastTixGiftWasRefund[, prevGiftsSentence :=  fifelse(dtrCounts == 1,
                                          "kindly donating your Season 27 ticket refund back to RTP.",
                                          "kindly donating your Season 27 ticket refunds back to RTP.")]

#Drop accounts in lastSubGiftWasRefund & lastTixGiftWasRefund from giftsByAcct
remRefundGifts <- giftsByAcct[!(accountID %in% lastSubGiftWasRefund$accountID),]
remRefundGifts <- remRefundGifts[!(accountID %in% lastTixGiftWasRefund$accountID),]

#Create table of donors who donated their subs refund from s27 and made other gifts
srDonatedPlus <- remRefundGifts[!is.na(donatedSubRefund),] #57 donors
#Create table of donors who donated their ticket refund from s27 and made other gifts
trDonatedPlus <- remRefundGifts[!is.na(donatedTixRefund),] #3 donors

srd1 <- srDonatedPlus[!is.na(gtnCounts)&
                !is.na(godCounts)&
                !is.na(saCounts)&
                is.na(rgCounts)&
                is.na(rRefundCount),] #1
srd1[, prevGiftsSentence := paste0(springAppeal,", ",gtn," and ",god,".")]

srd2 <- srDonatedPlus[is.na(gtnCounts)&
                !is.na(godCounts)&
                !is.na(saCounts)&
                is.na(rgCounts)&
                is.na(rRefundCount),] #2
srd2[, prevGiftsSentence := paste0(springAppeal," and ",god,".")]

srd3 <- srDonatedPlus[!is.na(gtnCounts)&
                is.na(godCounts)&
                !is.na(saCounts)&
                is.na(rgCounts)&
                is.na(rRefundCount),] #3
srd3[, prevGiftsSentence := paste0(springAppeal," and ",gtn,".")]

srd4 <- srDonatedPlus[!is.na(gtnCounts)&
                !is.na(godCounts)&
                is.na(saCounts)&
                is.na(rgCounts)&
                is.na(rRefundCount),] #8
srd4[, prevGiftsSentence := paste0(gtn," and ",god,".")]

srd5 <- srDonatedPlus[!is.na(gtnCounts)&
                is.na(godCounts)&
                is.na(saCounts)&
                !is.na(rgCounts)&
                is.na(rRefundCount),] #2
srd5[, prevGiftsSentence := paste0(gtn," and ",recentGift,".")]

srd6 <- srDonatedPlus[!is.na(gtnCounts)&
                is.na(godCounts)&
                is.na(saCounts)&
                is.na(rgCounts)&
                !is.na(rRefundCount),] #1
srd6[, prevGiftsSentence := paste0(gtn," and ",recentRefundGift,".")]

srd7 <- srDonatedPlus[is.na(gtnCounts)&
                is.na(godCounts)&
                !is.na(saCounts)&
                is.na(rgCounts)&
                !is.na(rRefundCount),] #1
srd7[, prevGiftsSentence := paste0(springAppeal," and ",recentRefundGift,".")]

srd8 <- srDonatedPlus[is.na(gtnCounts)&
                !is.na(godCounts)&
                is.na(saCounts)&
                is.na(rgCounts)&
                !is.na(rRefundCount),] #1
srd8[, prevGiftsSentence := paste0(god," and ",recentRefundGift,".")]

srd9 <- srDonatedPlus[!is.na(gtnCounts)&
                !is.na(godCounts)&
                is.na(saCounts)&
                !is.na(rgCounts)&
                is.na(rRefundCount),] #2
srd9[, prevGiftsSentence := paste0(gtn,", ",god," and ",recentGift,".")]

srd10 <- srDonatedPlus[!is.na(gtnCounts)&
                !is.na(godCounts)&
                is.na(saCounts)&
                is.na(rgCounts)&
                !is.na(rRefundCount),] #2
srd10[, prevGiftsSentence := paste0(gtn,", ",god," and ",recentRefundGift,".")]

srd11 <- srDonatedPlus[!is.na(gtnCounts)&
                is.na(godCounts)&
                is.na(saCounts)&
                is.na(rgCounts)&
                is.na(rRefundCount),] #5
srd11[, prevGiftsSentence := paste0(gtn,".")]

srd12 <- srDonatedPlus[is.na(gtnCounts)&
                !is.na(godCounts)&
                is.na(saCounts)&
                is.na(rgCounts)&
                is.na(rRefundCount),] #9
srd12[, prevGiftsSentence := paste0(god,".")]

srd13 <- srDonatedPlus[is.na(gtnCounts)&
                is.na(godCounts)&
                !is.na(saCounts)&
                is.na(rgCounts)&
                is.na(rRefundCount),] #7
srd13[, prevGiftsSentence := paste0(springAppeal,".")]

srd14 <- srDonatedPlus[is.na(gtnCounts)&
                is.na(godCounts)&
                is.na(saCounts)&
                !is.na(rgCounts)&
                is.na(rRefundCount),] #5
srd14[, prevGiftsSentence := paste0(recentGift,".")]

srd15 <- srDonatedPlus[is.na(gtnCounts)&
                is.na(godCounts)&
                !is.na(saCounts)&
                !is.na(rgCounts)&
                is.na(rRefundCount),] #1
srd15[, prevGiftsSentence := paste0(springAppeal," and ",recentGift,".")]

srd16 <- srDonatedPlus[is.na(gtnCounts)&
                !is.na(godCounts)&
                is.na(saCounts)&
                !is.na(rgCounts)&
                !is.na(rRefundCount),] #3
srd16[, prevGiftsSentence := paste0(god,", ",recentGift," and ",recentRefundGift,".")]

srd17 <- srDonatedPlus[is.na(gtnCounts)&
                !is.na(godCounts)&
                !is.na(saCounts)&
                !is.na(rgCounts)&
                !is.na(rRefundCount),] #1
srd17[, prevGiftsSentence := paste0(springAppeal,", ",god," and ",recentGift,".")]

srd18 <- srDonatedPlus[!is.na(gtnCounts)&
                !is.na(godCounts)&
                is.na(saCounts)&
                !is.na(rgCounts)&
                !is.na(rRefundCount),] #1
srd18[, prevGiftsSentence := paste0(gtn,", ",god," and ",recentGift,".")]

srd19 <- srDonatedPlus[is.na(gtnCounts)&
                !is.na(godCounts)&
                is.na(saCounts)&
                !is.na(rgCounts)&
                is.na(rRefundCount),]
srd19[, prevGiftsSentence := paste0(god," and ",recentGift,".")]

#join the srds in one table
srDonors <- rbind(srd1, srd2, srd3, srd4, srd5, srd6, srd7, srd8, srd9, srd10, 
                  srd11, srd12, srd13, srd14, srd15, srd16, srd17, srd18, srd19, lastSubGiftWasRefund)

trDonatedPlus[, prevGiftsSentence := fifelse(!is.na(gtnCounts)&
                                               is.na(saCounts)&
                                               is.na(godCounts)&
                                               is.na(rgCounts)&
                                               is.na(rRefundCount),
                                           paste0(gtn," and ",donatedTixRefund),
                                           fifelse(is.na(gtnCounts)&
                                                     is.na(saCounts)&
                                                     !is.na(godCounts)&
                                                     is.na(rgCounts)&
                                                     !is.na(rRefundCount),
                                                   paste0(god,", ",recentRefundGift," and ",donatedTixRefund),
                                                   paste0(springAppeal,", ",god," and ",donatedTixRefund)))]
#Combine the refund donor records
refundDonors <- rbind(trDonatedPlus, lastTixGiftWasRefund, srDonors)

#Drop accounts in refundDonors from giftsByAcct
remCurDonors <- giftsByAcct[!(accountID %in% refundDonors$accountID),]

remCurDonors[!is.na(gtnCounts)&
               !is.na(godCounts)&
               !is.na(saCounts)&
               is.na(rgCounts)&
               is.na(rRefundCount), prevGiftsSentence := paste0(springAppeal,", ",gtn," and ",god,".")]

remCurDonors[!is.na(gtnCounts)&
               is.na(godCounts)&
               is.na(saCounts)&
               is.na(rgCounts)&
               is.na(rRefundCount), prevGiftsSentence := paste0(gtn,".")]

remCurDonors[is.na(gtnCounts)&
               !is.na(godCounts)&
               is.na(saCounts)&
               is.na(rgCounts)&
               is.na(rRefundCount), prevGiftsSentence := paste0(god,".")]

remCurDonors[is.na(gtnCounts)&
               is.na(godCounts)&
               !is.na(saCounts)&
               is.na(rgCounts)&
               is.na(rRefundCount), prevGiftsSentence := paste0(springAppeal,".")]

remCurDonors[is.na(gtnCounts)&
               is.na(godCounts)&
               is.na(saCounts)&
               !is.na(rgCounts)&
               is.na(rRefundCount), prevGiftsSentence := paste0(recentGift,".")]

remCurDonors[is.na(gtnCounts)&
               is.na(godCounts)&
               is.na(saCounts)&
               is.na(rgCounts)&
               !is.na(rRefundCount), prevGiftsSentence := paste0(recentRefundGift,".")]

remCurDonors[!is.na(gtnCounts)&
               !is.na(godCounts)&
               is.na(saCounts)&
               is.na(rgCounts)&
               is.na(rRefundCount), prevGiftsSentence := paste0(gtn," and ",god,".")]

remCurDonors[!is.na(gtnCounts)&
               is.na(godCounts)&
               !is.na(saCounts)&
               is.na(rgCounts)&
               is.na(rRefundCount), prevGiftsSentence := paste0(springAppeal," and ",gtn,".")]

remCurDonors[!is.na(gtnCounts)&
               is.na(godCounts)&
               is.na(saCounts)&
               !is.na(rgCounts)&
               is.na(rRefundCount), prevGiftsSentence := paste0(gtn," and ",recentGift,".")]

remCurDonors[!is.na(gtnCounts)&
               is.na(godCounts)&
               is.na(saCounts)&
               is.na(rgCounts)&
               !is.na(rRefundCount), prevGiftsSentence := paste0(gtn," and ",recentRefundGift,".")]

remCurDonors[is.na(gtnCounts)&
               !is.na(godCounts)&
               !is.na(saCounts)&
               is.na(rgCounts)&
               is.na(rRefundCount), prevGiftsSentence := paste0(springAppeal," and ",god,".")]

remCurDonors[is.na(gtnCounts)&
               !is.na(godCounts)&
               is.na(saCounts)&
               !is.na(rgCounts)&
               is.na(rRefundCount), prevGiftsSentence := paste0(god," and ",recentGift,".")]

remCurDonors[is.na(gtnCounts)&
               !is.na(godCounts)&
               is.na(saCounts)&
               is.na(rgCounts)&
               !is.na(rRefundCount), prevGiftsSentence := paste0(god," and ",recentRefundGift,".")]

remCurDonors[is.na(gtnCounts)&
               is.na(godCounts)&
               !is.na(saCounts)&
               !is.na(rgCounts)&
               is.na(rRefundCount), prevGiftsSentence := paste0(springAppeal," and ",recentGift,".")]

remCurDonors[is.na(gtnCounts)&
               is.na(godCounts)&
               is.na(saCounts)&
               !is.na(rgCounts)&
               !is.na(rRefundCount), prevGiftsSentence := paste0(recentGift," and ",recentRefundGift,".")]

remCurDonors[is.na(gtnCounts)&
               is.na(godCounts)&
               !is.na(saCounts)&
               is.na(rgCounts)&
               !is.na(rRefundCount), prevGiftsSentence := paste0(springAppeal," and ",recentRefundGift,".")]

remCurDonors[!is.na(gtnCounts)&
               is.na(godCounts)&
               is.na(saCounts)&
               !is.na(rgCounts)&
               !is.na(rRefundCount), prevGiftsSentence := paste0(gtn,", ",recentGift," and ",recentRefundGift,".")]

remCurDonors[!is.na(gtnCounts)&
               is.na(godCounts)&
               !is.na(saCounts)&
               !is.na(rgCounts)&
               is.na(rRefundCount), prevGiftsSentence := paste0(springAppeal,", ",gtn," and ",recentGift,".")]

remCurDonors[!is.na(gtnCounts)&
               !is.na(godCounts)&
               is.na(saCounts)&
               !is.na(rgCounts)&
               is.na(rRefundCount), prevGiftsSentence := paste0(gtn,", ",god," and ",recentGift,".")]

remCurDonors[!is.na(gtnCounts)&
               !is.na(godCounts)&
               !is.na(saCounts)&
               !is.na(rgCounts)&
               is.na(rRefundCount), prevGiftsSentence := paste0(springAppeal,", ",gtn," and ",god,".")]

remCurDonors[is.na(gtnCounts)&
               !is.na(godCounts)&
               !is.na(saCounts)&
               !is.na(rgCounts)&
               is.na(rRefundCount), prevGiftsSentence := paste0(springAppeal,", ",god," and ",recentGift,".")]

remCurDonors[!is.na(gtnCounts)&
               !is.na(godCounts)&
               is.na(saCounts)&
               !is.na(rgCounts)&
               !is.na(rRefundCount), prevGiftsSentence := paste0(gtn,", ",god," and ",recentRefundGift,".")]

#populate prevGiftSentence for the remaining rows
remCurDonors[is.na(prevGiftsSentence), prevGiftsSentence := paste0("your most recent annual fund contribution of $",lastDonationAmtPM," on ",wordDate,".")]
#Double check that all are now populated
remCurDonors[is.na(prevGiftsSentence),]

#Combine the segments of now-processed donor data:
currentDonors <- rbind(refundDonors, remCurDonors)

# Append lastGiftByCurDonor columns to the currentDonors datatable -- save it over currentDonors:
currentDonors <- merge(currentDonors, lastGiftByCurDonor, by.x = "accountID", by.y = "accountID", all.x = TRUE)

# Add current period donation totals to currentDonors dt by joining it with afDonorTotals on accountID
currentDonors <- merge(currentDonors, afDonorTotals, by.x = "accountID", by.y = "accountID", all.x = TRUE)
View(currentDonors)

#Add previous period donation totals to currentDonors
currentDonors <- merge(currentDonors, lpDonationTot, by.x = "accountID", by.y = "accountID", all.x = TRUE)

#Change nulls in lpDonationTotal column to 0
currentDonors$lpDonationTotal[is.na(currentDonors$lpDonationTotal)] <- 0

# Create new column showing donor levels:
currentDonors <- currentDonors %>%
  mutate(
    donorLevel = case_when(
      donationTotal<125 ~ "recent donors",
      donationTotal>=125 & donationTotal<250 ~ "RTP Supporter-level donors",
      donationTotal>=250 & donationTotal<500 ~ "RTP Investor-level donors",
      donationTotal>=500 & donationTotal<1000 ~ "RTP Underwriter-level donors",
      donationTotal>=1000 & donationTotal<2500 ~ "RTP Performer-level donors",
      donationTotal>=2500 & donationTotal<5000 ~ "RTP Director-level donors",
      TRUE ~ "RTP Producer-level donors"
    )
  )

#Calculate ask amounts: total for last gift period - current period total to date - rounded up to nearest 5:
currentDonors[, askAmt := ceiling((lpDonationTotal - donationTotal)/5)*5]

#Calculate matched ask amount:
currentDonors[, askMatched := askAmt*2]
#calculate donor total if donor gives ask amount
currentDonors[(donationTotal > 0) & (donorLevel == 'recent donors'), fullAskTotal := askAmt+donationTotal]

#Create askSentence column based on askAmt column values:
currentDonors[, askSentence := fifelse(askAmt >= 50,
                                       paste0(letterSalutation,", when you renew your annual fund support at its previous level with a gift of $",askAmt," (or more), your donation will DOUBLE to $",askMatched,"."),
                                       fifelse((askAmt > 0) & (askAmt < 50),
                                               paste0(letterSalutation,", when you renew your annual fund support with a gift of $50, your donation will DOUBLE to $100; better yet, your $100 gift becomes $200, and so on."),
                                               paste0(letterSalutation,", can we count on you to chip in $50 or more to double your gift's impact and help ensure RTP is financially prepared for any eventuality?")))]

#Create psSentence column based on askAmt column values:
currentDonors[, psSentence := fifelse((askAmt >= 50) & (donorLevel != 'recent donors'),
                                      paste0(letterSalutation,", your gift of $",askAmt," doubles to $",askMatched," and maintains your donor level designation, as outlined on the enclosed Stewardship Benefits & Privileges sheet. Rest assured, we’ll resume some of our donor events as soon as public health recommendations permit."),
                                      fifelse((askAmt > 0) & (askAmt < 50) & (donorLevel != 'recent donors'),
                                              paste0(letterSalutation,", your gift of $50 or more doubles your impact and maintains your donor level designation, as outlined on the enclosed Stewardship Benefits & Privileges sheet. Rest assured, we’ll resume some of our donor events as soon as public health recommendations permit."),
                                              fifelse((fullAskTotal >= 125) & (donorLevel == 'recent donors'),
                                                      paste0(letterSalutation,", your gift of $",askAmt," doubles to $",askMatched," and maintains your donor level designation, as outlined on the enclosed Stewardship Benefits & Privileges sheet. Rest assured, we’ll resume some of our donor events as soon as public health recommendations permit."),
                                                      fifelse((fullAskTotal < 125) & (donorLevel == 'recent donors'),
                                                              paste0("when you contribute to RTP's annual fund campaign by December 31, you can reduce your 2020 tax liability as permitted by law."),
                                                              paste0("when you contribute to RTP's annual fund campaign by December 31, you can reduce your 2020 tax liability as permitted by law.")))))]

# Confirm no rows exist where donation total is NA:
amountNA <- which(is.na(currentDonors[,donationTotal]))
View(amountNA)

#Add other necessary columns back to the dt
addCols <- unique(afCurrent[, .(accountID, doNotMail, emailOptOut, email, street, city, state, zip)], by='accountID')
currentDonors <- merge(currentDonors, addCols, by.x = "accountID", by.y = "accountID", all.x = TRUE)

# Find rows where street is na:
streetNACur <- which(is.na(currentDonors$street))
#Find rows where street is blank
streetBlankCur <- currentDonors[street == '',]

# Remove the records with blank or an @ in the Account Name or Address field:
which(str_detect(currentDonors$accountName, "@"))
currentDonors[accountName == '',]
which(str_detect(currentDonors$street, "@"))

#Add accountID to tickets dt (here to facilitate next operation)
tickets <- tickets[, c(1, 4, 5, 7, 8, 13, 14, 15, 16)] #select columns
ticketsAccts <- merge(tickets, accounts, by.x = "contactID", by.y = "contactID", all.x = TRUE)

#Join missing address record with ticket data to populate addr fields where possible & dedupe on accountID
addrsTicketAccts <- ticketsAccts[, .(contactID, accountID, firstName.x, lastName.x, street, city, state, zip, email,
                                     createdDate)][order(accountID, -createdDate), head(.SD, 1), by='accountID']
#Repair missing address fields for donors identified above
#Remove street column from streetBlankCur so only one such column remains after repair
streetBlankCur[, `:=`(street = NULL,
                      city = NULL,
                      state = NULL,
                      zip = NULL)]
streetAddedCur <- merge(streetBlankCur, addrsTicketAccts, by.x = 'accountID', by.y = 'accountID', all.x = TRUE)
# #How many addresses were repaired
streetAddedCur[!(is.na(street)) & (street != ''),] #only 4
#Use the Give OUT Day data from Spring/Summer 2020 for completing more of the missing addresses
streetAddedCur <- merge(streetBlankCur, gOUTDay2020, by.x = 'email', by.y = 'email', all.x = TRUE) ##THIS DOUBLED A RECORD - COME BACK TO DISCERN WHY & FIX
streetStillBlank <- streetAddedCur[(is.na(street)) | (street == ''),][, `:=`(firstName = NULL,
                                                                             lastName = NULL,
                                                                             street = NULL,
                                                                             city = NULL,
                                                                             state = NULL,
                                                                             zip = NULL)]
streetAddedCur <- streetAddedCur[!(is.na(street)) | (street != ''),]
#Combine firstName and lastName into accountName for gOUTDay2020 dt to allow joining
gOUTDay2020$accountName <- paste(gOUTDay2020$firstName,gOUTDay2020$lastName)
streetAddedCur2 <- merge(streetStillBlank, gOUTDay2020, by.x = 'accountName', by.y = 'accountName', all.x = TRUE)[!(is.na(street)) | (street != ''),]
#Prepare streetAddedCur and streetAddedCur2 to add back with currentDonors table
streetAddedCur <- streetAddedCur[, .(accountID, street, city, state, zip, firstName, lastName)]
streetAddedCur2 <- streetAddedCur2[, .(accountID, street, city, state, zip, firstName, lastName)]
streetAddedCur <- rbind(streetAddedCur, streetAddedCur2)

addrFixCurDon <- currentDonors[accountID %in% streetBlankCur$accountID,][, `:=`(street = NULL,
                                                                                city = NULL,
                                                                                state = NULL,
                                                                                zip = NULL)]

addrFixCurDon <- merge(addrFixCurDon, streetAddedCur, by.x = 'accountID', by.y = 'accountID', all.x = TRUE)[!(is.na(street)) | (street != ''),]
#Use the newly added firstName column to replace the previously created letterSalutation values
#addrFixCurDon[, letterSalutation := firstName]   
fixedCurDon <- currentDonors[(accountID %in% addrFixCurDon$accountID),][,`:=`(letterSalutation = NULL,
                                                                              street = NULL,
                                                                              city = NULL,
                                                                              state = NULL,
                                                                              zip = NULL)]
#Select columns from addrFixCurDon that we need to transfer to fixedCurDon
addrFixCurDon <- addrFixCurDon[, .(accountID, street, city, state, zip, letterSalutation)]
fixedCurDon <- merge(fixedCurDon, addrFixCurDon, by.x = 'accountID', by.y = 'accountID', all.x = TRUE )
#Remove pre-fixed records from currentDonors
currentDonors <- currentDonors[!(accountID %in% streetBlankCur$accountID),]
#append fixedCurDon to currentDonors
currentDonors <- unique(rbind(currentDonors, fixedCurDon), by='accountID')

# Find the current donors that are marked "do Not Mail"
doNotMailCurrent <- currentDonors[doNotMail == "1",]

# Save the current donor direct mail records (those without do Not Mail marked)
afCurrent4DM <- currentDonors[doNotMail == "0",]

# Fix the row "names" (which are actually index numbers) to account for the two rows that were removed:
rownames(afCurrent4DM) <-seq(length=nrow(afCurrent4DM))

#Select the necessary columns:
afCurrentDirectMail <- afCurrent4DM[, c(1, 2, 4, 22, 23, 28:40)]

#Select longest and shortest drop-ins for graphic designer
# which(nchar(afCurrentDirectMail$prevGiftsSentence) == max(nchar(afCurrentDirectMail$prevGiftsSentence)))
# which(nchar(afCurrentDirectMail$prevGiftsSentence) == min(nchar(afCurrentDirectMail$prevGiftsSentence)))
# which(nchar(afCurrentDirectMail$letterSalutation) == max(nchar(afCurrentDirectMail$letterSalutation)))
# which(nchar(afCurrentDirectMail$letterSalutation) == min(nchar(afCurrentDirectMail$letterSalutation)))
# which(nchar(afCurrentDirectMail$askSentence) == max(nchar(afCurrentDirectMail$askSentence)))
# which(nchar(afCurrentDirectMail$askSentence) == min(nchar(afCurrentDirectMail$askSentence)))
# which(nchar(afCurrentDirectMail$psSentence) == max(nchar(afCurrentDirectMail$psSentence)))
# which(nchar(afCurrentDirectMail$psSentence) == min(nchar(afCurrentDirectMail$psSentence)))
# which(nchar(afCurrentDirectMail$donorLevel) == max(nchar(afCurrentDirectMail$donorLevel)))
# which(nchar(afCurrentDirectMail$donorLevel) == min(nchar(afCurrentDirectMail$donorLevel)))

#####################################
# # # # # # LAPSED DONORS# # # # # # 
#####################################

# Create lastGiftByDonor datatable, containing only accountID and two renamed cols lastGiftDate and lastGiftAmt column, grouped by accountID for afLapsed
lastGiftByLapsedDonor <- afLapsed[
  order(-closeDate), head(.SD, 1), by = accountID][, 1:8][, .(accountID, lastGiftDate = closeDate, lastGiftAmt = amount)]

# Remove duplicates from the afLapsed datatable - keeping the most recent donation - to prepare remove donors in current dt:
uniqueLapsedDonors <- unique(afLapsed, by="accountID", fromLast = TRUE)
View(uniqueLapsedDonors)

#Find the rows in uniqueLapsedDonors that are not present in afCurrent 
#(use this so that we also suppress accountIDs ultimately marked as do not mail) based on accountID --> lapsed:
afLapsedNotInCurrent <- anti_join(uniqueLapsedDonors, afCurrent, by = "accountID")
View(afLapsedNotInCurrent)

# Append lastGiftByLapsedDonor columns to the afLapsedNotInCurrent datatable -- save it over afLapsed:
afLapsed <- merge(afLapsedNotInCurrent, lastGiftByLapsedDonor, by.x = "accountID", by.y = "accountID", all.x = TRUE)

# Find the lapsed donors that are marked "do Not Mail"
doNotMailLapsed <- afLapsed[doNotMail == "1",]

#Store all accountIDs in lapsed stream to include the do not mail accounts
afLapsedAll <- afLapsed

# Save the records without do Not Mail marked over the data table called afLapsed
afLapsed <- afLapsed[doNotMail != "1",]

# Fix the row "names" (which are actually index numbers) to account for the two rows that were removed:
rownames(afLapsed) <-seq(length=nrow(afLapsed))

# Remove unnecessary columns from afLapsed:
afLapsedDirectMail <- afLapsed[, c(1, 6, 9:14, 29, 31)]
View(afLapsedDirectMail)

# Remove the records with blank or an @ in the Account Name or Address field:
which(str_detect(afLapsedDirectMail$accountName, "@"))
afLapsedDirectMail[accountName == '',]
which(str_detect(afLapsedDirectMail$street, "@"))
afLapsedDirectMail[street == '',]
#Remove those found just above (correct info not available to populate these records)
afLapsedDirectMail <- afLapsedDirectMail[street != '',]

####################################
# # # # # # Ticket Buyers# # # # # #
####################################

#Add accountID to nonJoans and joans:
nonJoansAccts <- merge(nonJoans, accounts, by.x = "contactID", by.y = "contactID", all.x = TRUE)
joansAccts <- merge(joans, accounts, by.x = "contactID", by.y = "contactID", all.x = TRUE)

#Dedupe joans and nonJoans dts by accountID
nonJoansAccts <- unique(nonJoansAccts, by="accountID", fromLast=TRUE)
joansAccts <- unique(joansAccts, by="accountID", fromLast=TRUE)

#Create dt of accounts that purchased tickets to JoA, but not to any other show (since at least 7/1/2015)
#(for use as suppression file)
joanOnly <- anti_join(joansAccts, nonJoansAccts, by = "accountID") 

#Remove rows where contactID is blank ("")
ticketsAccts <- ticketsAccts[contactID != "",]
#Remove rows where contactID is na
ticketsAccts <- ticketsAccts[!is.na(contactID),]
#Remove rows where accountID is na
ticketsAccts <- ticketsAccts[!is.na(accountID),]
#Remove rows where accountID is blank ("")
ticketsAccts <- ticketsAccts[contactID != "",]

# Remove duplicates from the ticketsAccts datatable - keeping the most recent ticket purchase record
uniqueTicketBuyers <- unique(ticketsAccts[order(accountID, -createdDate),], by="accountID", fromLast = TRUE)

#Find the accounts in uniqueTicketBuyers that are not present in afCurrent based on accountID:
afTBNotInCurrent <- anti_join(uniqueTicketBuyers, afCurrentDirectMail, by = "accountID")

#Find the rows in afTBNotInCurrent that are not present in afLapsedAll based on accountID:
afTBNotInCurrentOrLapsed <- anti_join(afTBNotInCurrent, afLapsedDirectMail, by = "accountID")

#Append amountDonated and amountCredited fields from subscriber datatable to afTBNotInCurrentOrLapsed:
afTBNotInCurrentOrLapsed <- merge(afTBNotInCurrentOrLapsed, subscriberCols, by.x = "accountID", by.y = "accountID", all.x = TRUE)

#Propercase the First Name and Last Name fields
afTBNotInCurrentOrLapsed$firstName.x <- str_to_title(afTBNotInCurrentOrLapsed$firstName.x)
afTBNotInCurrentOrLapsed$lastName.x <- str_to_title(afTBNotInCurrentOrLapsed$lastName.x)
afTBNotInCurrentOrLapsed$accountName <- str_to_title(afTBNotInCurrentOrLapsed$accountName)
afTBNotInCurrentOrLapsed$acctInfSal <- str_to_title(afTBNotInCurrentOrLapsed$acctInfSal)

#Fix the capitalized Ands introduced through the propercasing above
afTBNotInCurrentOrLapsed[, accountName := str_replace(accountName," And "," and ")]
afTBNotInCurrentOrLapsed[, acctInfSal := str_replace(acctInfSal," And "," and ")]

#Remove the records with an @ in the Account Name field (if any):
which(str_detect(afTBNotInCurrentOrLapsed$accountName, "@"))
which(afTBNotInCurrentOrLapsed$accountName == '')

#Review dt with the identified records from accountName check:
tix_accts_4imp <- afTBNotInCurrentOrLapsed[str_detect(accountName, "@"),]
View(tix_accts_4imp) #780  781  782 1999 2000 2004 2005 2174 2175 2176 2177 2178 2179 2181 2182

#Handle records found in accountName inquiry above:
afTBNotInCurrentOrLapsed$accountName[XXX] <- "XXX XXXXXXX"

#confirm accountName updates were effective
afTBNotInCurrentOrLapsed[(accountID %in% tix_accts_4imp$accountID),]

# #Id ticketbuyers with multiple contacts - to inform salutation creation
# afHHTB <- afTBNotInCurrentOrLapsed[(str_detect(afTBNotInCurrentOrLapsed$accountName, " And ")),]
# 
# #Id ticketbuyer accounts with a single contact - informs salutation creation
# afTBNotInCurrentOrLapsed <- anti_join(afTBNotInCurrentOrLapsed, afHHTB, by = 'accountID')

#Create builtSalutation field to serve as default value for records where acctInfSal is missing:
afTBNotInCurrentOrLapsed <- cbind(afTBNotInCurrentOrLapsed, data.table(str_match(afTBNotInCurrentOrLapsed$accountName,"(([A-Z][a-z]+\\sand\\s[A-Z][a-z]+)\\s[A-Z]?'?[A-z]+)|(([A-Z][a-z]+)\\s[A-Z][a-z]+\\sand\\s([A-Z][a-z]+)\\s[A-Z][a-z]+)|([A-Z][a-z]+)\\s[A-Z][a-z]+"))[
  ,c("V1", 'V3', "V5", 'V6', 'V7')][, builtSalutation := ifelse(!is.na(V5),
                                                                paste0(V5," and ",V6),
                                                                ifelse(!is.na(V7),
                                                                       paste0(V7),
                                                                       paste0(V3)))][, .(builtSalutation)])

#Add letterSalutation column to afTBNotInCurrentOrLapsed:
afTBNotInCurrentOrLapsed[, letterSalutation := fifelse(acctInfSal != "",
                                       acctInfSal,
                                       fifelse(tixOrderInfSal != "",
                                               tixOrderInfSal,
                                               builtSalutation))]



# Remove the records with an @ in the Address field:
which(str_detect(afTBNotInCurrentOrLapsed$street, "@")) #zero found - no imputation needed here
#Remove records where street == deceased (future implementation)
#afTBNotInCurrentOrLapsed <- afTBNotInCurrentOrLapsed[street != 'Deceased',]
#Where possible fix records where street is blank
needAddr <- accounts[accountID %in% (afTBNotInCurrentOrLapsed[street == '', accountID]),]
afTBNotInCurrentOrLapsed$street[XXXX] <- 'XXXX XXXXXXX' #removed donor specific info
rm(needAddr)
afTBNotInCurrentOrLapsed <- afTBNotInCurrentOrLapsed[street != '',]

#Create patronType column where na is ticket buyers and 1 is subscribers
afTBNotInCurrentOrLapsed$patronType[is.na(afTBNotInCurrentOrLapsed$subscriber)] <- "ticket buyers"
#Map 1 in subscriber col to "subscribers"
afTBNotInCurrentOrLapsed[subscriber == 1, patronType := "subscribers"]
#Check output
View(afTBNotInCurrentOrLapsed)

############################################################
######Remove joan of arc only records from all segments#####
############################################################

#Suppress any accounts in joanOnly from afTBNotInCurrentOrLapsed:
afTixSubsDM <- afTBNotInCurrentOrLapsed[!(accountID %in% joanOnly$accountID),]

#keep the subscribers
afSubs <- afTixSubsDM[patronType == 'subscribers',]
afTixSubsDM <- afTixSubsDM[patronType != 'subscribers',]

#Reduce record count of afTixSubsDM by 748 (2576-748 = 1828) to accommodate the amount of 
#materials we have for the mailing
afTixSubsDM <- afTixSubsDM[city != '',]
afTixSubsDM <- afTixSubsDM[!(priceLevelName %in% c('Comp', 'Performer and Staff Comps', 'Performer Comps', 'Press and VIP Seats', 'VIP and Press', 'VIP and Press Comps', 'VIP and Press Seats', 'VIP and Press Seat')),]
afTixSubsDM <- afTixSubsDM[priceLevelName == 'Regular Price',]
afTixSubsDM <- afTixSubsDM[state %in% c('VA', 'NC', 'DC', "MD"),]
afTixSubsDM <- afTixSubsDM[createdDate > '2018-08-23',] #leaves 1806

#add subscribers back in
afTixSubsDM <- rbind(afTixSubsDM, afSubs)

#Which Joan of Arc ticket buyers are not also present in afTBNotInCurrentOrLapsed?
#(i.e. which accounts purchased only tickets to joan of arc -> joansNotInTB)
setnames(joanOnly, "accountName.x", "accountName")
joansNotInTB <- anti_join(joanOnly, afTBNotInCurrentOrLapsed, by="accountID")[, .(accountID, accountName)]
#Show the joan of arc ticket buyer accounts that were actually suppressed from the ticketbuyer segment
tbRemoved <- anti_join(joanOnly, joansNotInTB, by='accountID')[, .(accountID, accountName)]
#Records in joansNotInTB were not present in the ticketbuyers/subs segment because they made donations
#made within the past year so they are in the current donor segment. However, 2 of the 9 records made
#a donation not associated with their joan of arc ticket purchase. Keep the other 7 to drop from current segment
joanDropInCurr <- joansNotInTB[3:9,]
#Drop the 7 records with donations made in conjunction with joan ticket purchase and suppress the rest from the current donor segment above
afCurrentDirectMail <- anti_join(afCurrentDirectMail, joanDropInCurr, by='accountID')

###########################################################
# # # # # # Export Final Segment Files to Excel # # # # # # 
###########################################################
# Save afCurrentDirectMail to Excel as Currentdonors:
write_xlsx(afCurrentDirectMail, "/Users/georgesmith/Desktop/RTP/Season 28/Development/Fall Annual Fund 2020/AFMailing_fall2020_CurrentDonors_Final.xlsx", col_names = TRUE, format_headers = TRUE) 

# Save afLapsedDirectMail to Excel as AFMailing_fall2020_LapsedDonors_Final.xlsx:
write_xlsx(afLapsedDirectMail, "/Users/georgesmith/Desktop/RTP/Season 28/Development/Fall Annual Fund 2020/AFMailing_fall2020_LapsedDonors_Final.xlsx", col_names = TRUE, format_headers = TRUE) 

# Save afTixSubsDM to Excel as AFMailing_fall2020_TicketBuyers&Subs_Final.xlsx:
write_xlsx(afTixSubsDM, "/Users/georgesmith/Desktop/RTP/Season 28/Development/Fall Annual Fund 2020/AFMailing_fall2020_TicketBuyers&Subs_Final.xlsx", col_names = TRUE, format_headers = TRUE) 
