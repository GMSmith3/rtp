#Set working directory
setwd("/Users/georgesmith/Desktop/RTP/Season 28/Development/GivingTuesdayNow/Spring Appeal 2020")
#Check working directory
getwd()
#Load the packages I know I'll need:
library(readr)
library(data.table)
library(dplyr)
library(writexl)

#Let's import the dataframes containing AF donors and ticket buyers (7/1/17 - 4/24/19):
afDonors <- fread("AFDonations_Jul2017_May142020.csv")
afTickets <- fread("Ticket_Buyers_S27.csv")
accounts <- fread("AllAccountsandContacts_April20_2020.csv")
subscribers <- fread("Subscribers_2019_2020_for_Exchange.csv")

#Calculate the sum of Amount column for every unique AccountID. Call the new data - donationTotalDT:
#Add na.rm = TRUE to the sum function to avoid sum being reported as NA for accounts with an NA donation amount
afDonorTotalsDT <- afDonorsDT[,.(donationTotal = sum(Amount, na.rm=TRUE)),by=AccountID]
View(afDonorTotalsDT)
#Remove duplicates from the afDonorsDT data table to prepare it to join with afDonorTotalsDT
uniqueafDonorsDT <- unique(afDonorsDT, by="AccountID")
View(uniqueafDonorsDT)
#Join uniqueafDonorsDT and afDonorTotalsDT on AccountID
afDonorsDT <- merge(uniqueafDonorsDT, afDonorTotalsDT, by.x = "AccountID", by.y = "AccountID")
View(afDonorsDT)
#Find the current donors that are marked "do Not Mail"
doNotMailCurrent <- afDonorsDT[doNotMail == "1"]
#Save the records without do Not Mail marked over the data table called afDonorsDT
afDonorsDT <- afDonorsDT[doNotMail == "0"]
#Create current donor list of businesses
afBusCurrentDT <- afDonorsDT[AccountRecordType == "Business"]
#Create current donor data table that includes only households and individuals:
afDonorsDT <- afDonorsDT[AccountRecordType %in% c("Household", "Individual")]
#Remove unnecessary columns from afDonorsDT:
afDonorsDTreduced <- afDonorsDT[, -c(3:4, 6:9, 16:18)]
View(afDonorsDTreduced)
afDonorsDT <- afDonorsDTreduced
rm(afDonorsDTreduced)
#Remove unnecessary columns from afBusCurrentDT
afBusCurrentDT <- afBusCurrentDT[, -c(3:4, 6:9, 16:18)]
#Filter the data table to include only records with donationTotal >= 125:
StewardsDT <- afDonorsDT[donationTotal >= 125,]
#Sort by donation total:
StewardsDT <- StewardsDT[order(donationTotal)]
View(StewardsDT)
#Filter the data table to include only records with donationTotal < 125:
smallDsDT <- afDonorsDT[donationTotal < 125,]
#Sort by donation total:
smallDsDT <- smallDsDT[order(donationTotal)]
View(smallDsDT)
#Fill in the donation totals for the two records that were mistakening coerced into NAs (AccountIDs: 0013600000VKRpL and 0013600000VKRpL)

#Find rows where "Amount" is NA in afDonors:
View(amountNA)
#Create new column showing donor levels:
afDonorsDT <- afDonorsDT %>%
  mutate(
    Level = case_when(
      donationTotal<125 ~ "a donor",
      donationTotal>=125 & donationTotal<250 ~ "a Supporter-level donor",
      donationTotal>=250 & donationTotal<500 ~ "an Investor-level donor",
      donationTotal>=500 & donationTotal<1000 ~ "an Underwriter-level donor",
      donationTotal>=1000 & donationTotal<2500 ~ "a Performer-level donor",
      donationTotal>=2500 & donationTotal<5000 ~ "a Director-level donor",
      TRUE ~ "a Producer-level donor"
    )
  )
View(afDonorsDT)
#Confirm no rows exist where donation total is NA:
amountNA <- which(is.na(afDonorsDT$donationTotal))
#Read in salutations file to join with donor files (as well as any others for which I can make a join work):
salutations <- read.xlsx(xlsxFile = "donorSalutations.xlsx", sheet = "Sheet1")
#Make it a data table:
salutationsDT <- data.table(salutations)
#Remove dupes from salutation data table before merging to save some time and the need to edit that file.
salutationsDT <- unique(salutationsDT, by = "AccountID")
#Add salutations to afDonorsDT:
afDonorsDTS <- merge(afDonorsDT, salutationsDT, by.x = "AccountID", by.y = "AccountID", all.x = TRUE)
#Create salutations for records whose salutations fields are NA:
afDonorsDT <- afDonorsDT %>% 
  mutate(
    LetterSalutation = ifelse(test = is.na(Salutation),
                              yes = AccountName,
                              no = Salutation)
  )
#Remove donors at $5k and over from the list (we'll handle these manually):
afDonorsDTS <- afDonorsDTS[afDonorsDTS$donationTotal < 5000,]
afDonorsDT <- afDonorsDTS
rm(afDonorsDTS)
#Find rows where MailingStreet is blank:
mailingstreetNA <- which(is.na(afDonorsDT$MailingStreet))
#Creaet a new field to populate the appeal letter with the donation amount required to take the donor to the next stewardship level:
afDonorsDT <- afDonorsDT %>% 
  mutate(
    ToNextLevel = ifelse(test = (donationTotal < 50),
                         yes = "$25 or $50",
                         no = ifelse(test = (donationTotal >= 50 & donationTotal < 125),
                                     yes = 125 - donationTotal,
                                     no = ifelse(test = (donationTotal >= 125 & donationTotal < 250),
                                                 yes = 250 - donationTotal,
                                                 no = ifelse(test = (donationTotal >= 250 & donationTotal < 500),
                                                             yes = 500 - donationTotal,
                                                             no = ifelse(test = (donationTotal >= 500 & donationTotal < 1000),
                                                                         yes = 1000 - donationTotal,
                                                                         no = ifelse(test = (donationTotal >= 1000 & donationTotal < 2500),
                                                                                     yes = 2500 - donationTotal,
                                                                                     no =  ifelse(test = (donationTotal >= 2500 & donationTotal < 5000),
                                                                                                  yes = 5000 - donationTotal,
                                                                                                  no = "your choosing")))))))
  )
#Create a new field with the next stewardship level for each record
afDonorsDT <- afDonorsDT %>%
  mutate(
    NextLevelName = case_when(
      donationTotal < 62.5 ~ "satisfaction in knowing you took at stand for equality and inclusion",
      donationTotal >= 62.5 & donationTotal<125 ~ "Supporter-level donor benefits (refer to the enclosed Stewardship Benefits & Privileges sheet)",
      donationTotal>=125 & donationTotal<250 ~ "Investor-level donor benefits (refer to the enclosed Stewardship Benefits & Privileges sheet)",
      donationTotal>=250 & donationTotal<500 ~ "Underwriter-level donor benefits (refer to the enclosed Stewardship Benefits & Privileges sheet)",
      donationTotal>=500 & donationTotal<1000 ~ "Performer-level donor benefits (refer to the enclosed Stewardship Benefits & Privileges sheet)",
      donationTotal>=1000 & donationTotal<2500 ~ "Director-level donor benefits (refer to the enclosed Stewardship Benefits & Privileges sheet)",
      donationTotal>=2500 & donationTotal<5000 ~ "Producer-level donor benefits (refer to the enclosed Stewardship Benefits & Privileges sheet)",
      TRUE ~ "the highest level of stewardship benefits (refer to the enclosed Stewardship Benefits & Privileges sheet)"
    )
  )

#Find max donation amount for each donor over the current donor period.
maxDonationsDT <- donorsDT[,.(maxDonation = max(Amount, na.rm = TRUE)), by=AccountID]
#Find mean donation amount for each donor over the current donor period.
AvgdonationsDT <- donorsDT[,.(Avgdonation = mean(Amount, na.rm = TRUE)), by=AccountID]
AvgdonationsDT <- AvgdonationsDT[-c(54, 141, 148),] 
#Remove the do not mail records from the maxdonations dt:
doNoMail <- afDonors[afDonors$doNotMail == "1",]
doNotMail <- doNoMail[-1,]
doNotMailAccts <- doNotMail$AccountID
maxDonationsDT <- maxDonationsDT[-c(54, 141, 148),] 
#Add the Average donation Column to afDonorsDT
afDonorsDT <- merge(afDonorsDT, AvgdonationsDT, by.x = "AccountID", by.y = "AccountID")
#Truncate avgdonation to remove the decimals (NEEDS TO BE FIXED):
#afDonorsDT[, Avgdonation1 := trunc(afDonorsDT$Avgdonation)]
#trunc(afDonorsDT$Avgdonation)
#####WILL NEED TO COME BACK AND ADD THESE ADDRESSES (above)
#Fix the row "names" (which are actually index numbers) to account for the two rows that were removed:
rownames(afDonorsDT) <-seq(length=nrow(afDonorsDT))
#Let's sort the data table in ascending order:
afDonorsDT <- afDonorsDT[order(afDonorsDT$donationTotal),]
View(afDonorsDT)
#Remove the records with an @ in the Account Name or Address field:
which(str_detect(afDonorsDT$AccountName, "@"))
which(str_detect(afDonorsDT$MailingStreet, "@"))
#Remove those records:
afDonorsDT <- afDonorsDT[-c(82, 134, 318),]
#Save afDonorsDT to Excel as Currentdonors & save afBusCurrentDT to Excel as CurrentBusinessdonors:
write.xlsx(afDonorsDT, file = "AFMailing_Currentdonors.xlsx", gridLines = TRUE) 
write.xlsx(afBusCurrentDT, file = "AFMailing_CurrentBusinessdonors.xlsx", gridLines = TRUE) 
######LAPSED DONORS######
#Remove duplicates from the afLapsedDT data table:
uniqueafLapsedDT <- unique(afLapsedDT, by="AccountID")
View(uniqueafLapsedDT)
#Save uniqueafLapsedDT back to afLapsedDT
afLapsedDT <- uniqueafLapsedDT
#Find the current donors that are marked "do Not Mail"
doNotMailLapsed <- afLapsedDT[do.not.mail == "1"]
#Save the records without do Not Mail marked over the data table called afDonorsDT
afLapsedDT <- afLapsedDT[do.not.mail != "1"]
#Create current donor list of businesses
afBusLapsedDT <- afLapsedDT[AccountRecordType == "Business"]
#Create current donor data table that includes on households and individuals:
afLapsedDT <- afLapsedDT[AccountRecordType %in% c("Household", "Individual")]
#Remove unnecessary columns from afLapsedDT:
afLapsedDTreduced <- afLapsedDT[, -c(3:4, 6:9, 16:18)]
View(afLapsedDTreduced)
afLapsedDT <- afLapsedDTreduced
#Which funds are reflected in this data table?
levels(factor(afLapsedDT$Fund))
#Filter our the rows where fund = "25TH"
WrongFundLapsed <- which(afLapsedDT$Fund == "25TH")
afLapsedDT <- afLapsedDT[-WrongFundLapsed,]
#Fix the row "names" (which are actually index numbers) to account for the three rows that were removed:
rownames(afLapsedDT) <-seq(length=nrow(afLapsedDT))
#Dedupe afLapsedDT against afDonorsDT using dplyr's anti_join function, which returns the rows of the
#first table where it cannot find a match in the second table
library(dplyr)
uniqueLapsedIDs <- anti_join(afLapsedDT, afDonorsDT, by = "AccountID")
afLapsedDT <- uniqueLapsedIDs
#Add level column to populate with "donor" for drop-in in the appeal letter:
afLapsedDT <- afLapsedDT %>% mutate(
  Level = "a donor"
)
#Add salutations to afLaspedDT:
afLapsedDTS <- merge(afLapsedDT, salutationsDT, by.x = "AccountID", by.y = "AccountID", all.x = TRUE)
#Create salutations for records whose salutations fields are NA:
afLapsedDT <- afLapsedDTS %>% 
  mutate(
    LetterSalutation = ifelse(test = is.na(Salutation),
                              yes = AccountName,
                              no = Salutation)
  )
#Find and remove email addresses from AccountName:
library(stringr)
str_detect(afLapsedDT$AccountName, "@")
str_detect(afLapsedDT$MailingStreet, "@")
#Remove the record that contained this:
afLapsedDT <- afLapsedDT[-5,]
#Save afLapseddonorsDT to Excel as Lapseddonors:
write.xlsx(afLapsedDT, file = "AFMailing_Lapseddonors.xlsx", gridLines = TRUE) 
#Remove unnecessary columns from afBusLapsedDT:
afBusLapsedDTreduced <- afBusLapsedDT[, -c(3:4, 6:9, 16:18)]
View(afBusLapsedDTreduced)
afBusLapsedDT <- afBusLapsedDTreduced
#Which funds are reflected in this data table?
levels(factor(afBusLapsedDT$Fund))
#All funds for which there are donation here are for the Annual Fund so no filtering on fund is required here.
#Fix the row "names" (which are actually index numbers) to account for the rows that were removed:
rownames(afBusLapsedDT) <-seq(length=nrow(afBusLapsedDT))
#Dedupe afLapsedDT against afBusLapsedDT using dplyr's anti_join function, which returns the rows of the
#first table where it cannot find a match in the second table
uniqueBusLapsedIDs <- anti_join(afBusLapsedDT, afBusCurrentDT, by = "AccountID") 
#No dupes across data tables so remove the afBusLapsedDT
rm(afBusLapsedDT, uniqueBusLapsedIDs)

######Ticket Buyers 7/1/17 - 4/24/19######
#Remove duplicates from the afTicketsDT data table (based on Conact ID):
uniqueafTicketsDT <- afTicketsDT %>% distinct(
  ContactID, .keep_all = TRUE
)
View(uniqueafTicketsDT)
#Remove duplicates from the afTicketsDT data table (based on Account):
uniqueAFacctTicketsDT <- afTicketsDT %>% distinct(
  Account, .keep_all = TRUE
)
View(uniqueAFacctTicketsDT)
#Save uniqueafTicketDT back to afTicketDT since Account has more empty fields than Contact ID
afTicketsDT <- uniqueafTicketsDT
#Remove the two objects holding the unique records:
rm(uniqueAFacctTicketsDT, uniqueafTicketsDT)
#What are the possible values in doNotMail?
levels(factor(afTicketsDT$doNotMail))
#Find the ticket buyers that are marked "doNotMail"
doNotMailTickets <- afTicketsDT[doNotMail == "1"]
#Find ticket buyers without do Not Mail marked over the data table called afTicketsDT
afTicketsMailOKDT <- afTicketsDT[doNotMail != "1"]
#Save the records without do Not Mail marked over the data table called afTicketsDT
afTicketsDT <- afTicketsMailOKDT
rm(afTicketsMailOKDT)
#Remove unnecessary columns from afTicketsDT:
afTicketsDTreduced <- afTicketsDT[, -c(2:3, 14:16, 18:20)]
View(afTicketsDTreduced)
afTicketsDT <- afTicketsDTreduced
rm(afTicketsDTreduced)
#Which ticket buyer types are reflected in this data table?
levels(factor(afTicketsDT$TicketOrderType))
#Use previously imported and joined donor object (from other script file) to add ContactID to the afDonorsDT 
#and afLapsedDT objects, which will then allow for duplicate removal across afTicketsDT
#First, name donors a data table
DTdonors <- data.table(donors)
uniqueDTdonors <- unique(DTdonors, by = "Account.ID")
#double check number of dupes to ensure the previous action yieled the correct result
dupedonorsDT <- DTdonors[duplicated(DTdonors$Account.ID),]
#Save unique records over DTdonors
DTdonors <- uniqueDTdonors
rm(uniqueDTdonors)
#Remove all columns except Account.ID (1), Contact.ID(4) and Account.Name(9)
reducedDTdonors <- DTdonors[,-c(2:3, 5:8, 10:49)]
DTdonors <-reducedDTdonors
rm(reducedDTdonors)
#Join DTdonors with afDonorsDT and afLapsedDT
afDonorsCidDT <- merge(afDonorsDT, DTdonors, by.x = "AccountID", by.y = "Account.ID")
afLapsedCidDT <- merge(afLapsedDT, DTdonors, by.x = "AccountID", by.y = "Account.ID")
#Rename ContactID (in afTicketsDT) to Contact.ID to match the field name in the two objects above
names(afTicketsDT)[names(afTicketsDT) == "ContactID"] <- "Contact.ID"
#Dedupe afTicketsDT against afDonorsCidDT using dplyr's anti_join function, which returns the rows of the
#first table where it cannot find a match in the second table
notCurrentDT <- anti_join(afTicketsDT, afDonorsCidDT, by = "Contact.ID")
ticketBuyersDT <- anti_join(notCurrentDT, afLapsedCidDT, by = "Contact.ID")
#Dedupe again: this time, use Account Name
notCurrentDT <- anti_join(afTicketsDT, afDonorsCidDT, by = c("Account" = "AccountName"))
ticketBuyersDT <- anti_join(notCurrentDT, afLapsedCidDT, by = c("Account" = "AccountName"))
#Add address fields to ticketBuyersDT:
accountsDT <- data.table(accounts)
ticketBuyersMailDT <- inner_join(ticketBuyersDT, accountsDT, by = "Account")
ticketBuyersNoAddr <- anti_join(ticketBuyersDT, ticketBuyersMailDT, by = "Account")
#Dedupe again; now on account id
notCurrentDT <- anti_join(ticketBuyersMailDT, afDonorsCidDT, by = "AccountID")
afticketBuyersMailDT <- anti_join(ticketBuyersMailDT, afLapsedCidDT, by = "AccountID")
#Remove some objects we no longer need
rm(notCurrentDT, afLapsedCidDT, afDonorsCidDT)
#Classify each ticket buyer as "loyal subscriber" or "someone who appreciates our productions"
#First, we need to see which values are present in the field (TicketOrderType) we'll query to create the new field:
levels(factor(afticketBuyersMailDT$TicketOrderType))
#Now, we'll create a new field called "Level" (to match the field name in the other data tables) with the ticket buyer class noted:
afticketBuyersMailDT <- afticketBuyersMailDT %>% 
  mutate(
  Level = case_when(
    TicketOrderType == "Tickets; Subscription" ~ "a loyal Subscriber",
    TRUE ~ "someone who appreciates our productions"
  )
)
#Add salutations to afticketBuyersMailDT:
afticketBuyersMailDT <- merge(afticketBuyersMailDT, salutationsDT, by.x = "AccountID", by.y = "AccountID", all.x = TRUE)
#Create salutations for records whose salutations fields are NA:
afticketBuyersMailDT <- afticketBuyersMailDT %>% 
  mutate(
    LetterSalutation = case_when(
      Account.Record.Type == "Individual" ~ First.Name,
      TRUE ~ Account
    )
  )
afticketBuyersMailDT <- afticketBuyersMailDT %>% 
  mutate(LetterSalutation = ifelse(test = (Account.Record.Type == "Individual" & is.na(ContactInformalSalutation)),
                                   yes = First.Name,
                                   no = ifelse(test = Account.Record.Type == "Individual",
                                               yes = ContactInformalSalutation,
                                               no = Account))
  )
  

#Remove the records with an @ in the Account Name or Address field:
which(str_detect(afticketBuyersMailDT$Account, "@"))
which(str_detect(afticketBuyersMailDT$MailingStreet, "@"))
afticketBuyersMailDT <- afticketBuyersMailDT[-c(326, 333, 350, 358, 1715, 1716, 1717, 1718, 1722, 1723, 1741, 1742),]
#Save ticketBuyersDT to excel:
write.xlsx(afticketBuyersMailDT, file = "AFMailing_ticketBuyers.xlsx", gridLines = TRUE) 
####NOTE:No subscribers were found in the ticket buyer only list (AKA all current subscribers are donors)
#Fix the row "names" (which are actually index numbers) to account for any rows that were removed:
rownames(ticketBuyersDT) <-seq(length=nrow(ticketBuyersDT))
#Append afDonorsDT to afLapsedDT:
#First, must make all columns match between the data.tables:
afLapsedDT <- afLapsedDT %>%
  mutate(
    donationTotal = "0"
    )
afLapsedDT$donationTotal <- as.numeric(as.character(afLapsedDT$donationTotal))
afLapsedDTtest <- afLapsedDT
#Now, append
donorUnion <- union(afDonorsDT, afLapsedDTtest)
#Create new object to hold these:
afAlldonorsDT <- donorUnion
rm(donorUnion)
#Save as Excel file:
write.xlsx(afAlldonorsDT, file = "AFMailing_donorList_Comprehensive.xlsx", gridLines = TRUE) 
