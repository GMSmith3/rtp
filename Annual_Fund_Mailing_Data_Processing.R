# Set working directory
setwd("/Users/georgesmith/Desktop/RTP/Season 28/Development/Spring Appeal 2020")
# Check working directory
getwd()
# Load the packages I know I'll need:
library(readr)
library(data.table)
library(dplyr)
library(writexl)
library(scales)
library(stringr)

# Let's import the datatables (dts) containing AF donors and ticket buyers (7/1/17 - 4/24/19):
afDonors <- fread("AFDonations_Jul2017_May202020.csv")
afTickets <- fread("Ticket_Buyers_S27.csv")
accounts <- fread("AllAccountsandContacts_April20_2020.csv")
subscribersDonateOrCredit <- fread("SubscribersCreditDispensation.csv")
curFall19Salutations <- fread("current_donors_fall2019_salutations.csv")

# Create consistent columns names across datatables:
colnames(afDonors) <- c("accountID", "accountRecordType", "doNotMail", "donationRecordType", "informalSalutation",
                        "accountName", "amount", "closeDate", "emailOptOut",  "email", "street", "city", "state",
                        "zip", "fund", "frequency", "paymentType", "pledgeAmount", "paymentSchedule", "type", "stage",
                        "fiscalYear", "lastDonationAmtPM", "lastDonationDatePM")

# Convert colnames in accounts dt to camel case to match those in other dts
colnames(accounts) <- c("firstName", "lastName", "contactID", "accountID", "accountName", "street", 
                        "city", "state", "zip",  "email", "phone")

# Create donor datatable that includes only households and individuals:
afDonors <- afDonors[accountRecordType %in% c("Household", "Individual")]

# Remove rows where either amount or lastDonationAmtPM is na (they mess up later processing)
afDonors <- afDonors[!is.na(amount)]
afDonors <- afDonors[!is.na(lastDonationAmtPM)]
# Convert date columns to Date datatype to allow for filtering on this feature:
afDonors$closeDate <- as.Date(afDonors$closeDate, format = "%m/%d/%y")
afDonors$lastDonationDatePM <- as.Date(afDonors$lastDonationDatePM, format = "%m/%d/%y")
afTickets$createdDate <- as.Date(afTickets$createdDate, format = "%m/%d/%y")

# Append salutation field to afDonors
afDonors <- merge(afDonors, curFall19Salutations, by.x = "accountID", by.y = "accountID", all.x = TRUE)
View(afDonors)

#Add subscriber column to subscribersDonateOrCredit to allow logic on that field after merge below
subscribersDonateOrCredit$subscriber <- 1

#Append amountDonated and amountCredited fields from subscriber datatable to afDonors:
subscriberCols <- subscribersDonateOrCredit[, 11:14]
afDonors <- merge(afDonors, subscriberCols, by.x = "accountID", by.y = "accountID", all.x = TRUE)

#Change nulls in subscriber column to 0
afDonors$subscriber[is.na(afDonors$subscriber)] <- 0

# # # # # # CURRENT DONORS (including CURRENT DONOR SUBSCRIBERS)# # # # # # 

# Break out donations from afDonors where closeDate < 07/01/2018, creating lapsed dt:
afLapsed <- afDonors[closeDate < "2018-07-01"][order(accountID, closeDate)] # sorting later simplifies selecting latest donation row for each accountID
afCurrent <- afDonors[closeDate > "2018-06-30"][order(accountID, closeDate)] # sorting later simplifies selecting latest donation row for each accountID

# Create lastGiftByDonor datatable, containing only accountID and two renamed cols lastGiftDate and lastGiftAmt column, grouped by accountID for afCurrent
lastGiftByCurDonor <- afCurrent[
  order(-closeDate), head(.SD, 1), by = accountID][, 1:8][, .(accountID, lastGiftDate = closeDate, lastGiftAmt = amount)]

# Create lastGiftByDonor datatable, containing only accountID and two renamed cols lastGiftDate and lastGiftAmt column, grouped by accountID for afLapsed
lastGiftByLapsedDonor <- afLapsed[
  order(-closeDate), head(.SD, 1), by = accountID][, 1:8][, .(accountID, lastGiftDate = closeDate, lastGiftAmt = amount)]

# Calculate the sum of amount column for every unique accountID. Call the new data - donationTotalDT:
# Add na.rm = TRUE to the sum function to avoid sum being reported as NA for accounts with an NA donation amount
afDonorTotals <- afCurrent[, .(donationTotal = sum(amount, na.rm=TRUE)), by = accountID]
View(afDonorTotals)

# Remove duplicates from the afCurrent datatable - keeping the most recent donation - to prepare it to join with afDonorTotals
uniqueCurDonors <- unique(afCurrent, by="accountID", fromLast = TRUE)
View(uniqueCurDonors)

# Remove duplicates from the afCurrent datatable - keeping the most recent donation - to prepare it to join with afDonorTotals
uniqueLapsedDonors <- unique(afLapsed, by="accountID", fromLast = TRUE)
View(uniqueLapsedDonors)

#Find the rows in uniqueLapsedDonors that are not present in uniqueCurDonors based on accountID --> lapsed:
afLapsedNotInCurrent <- anti_join(uniqueLapsedDonors, uniqueCurDonors, by = "accountID")
View(afLapsedNotInCurrent)

# Append lastGiftByCurDonor columns to the uniqueCurDonors datatable -- save it over afCurrent:
afCurrent <- merge(uniqueCurDonors, lastGiftByCurDonor, by.x = "accountID", by.y = "accountID", all.x = TRUE)

# Append lastGiftByLapsedDonor columns to the afLapsedNotInCurrent datatable -- save it over afCurrent:
afLapsed <- merge(afLapsedNotInCurrent, lastGiftByLapsedDonor, by.x = "accountID", by.y = "accountID", all.x = TRUE)

# Add donation totals to afCurrent dt by joining uniqueCurDonors and afDonorTotals on accountID
afCurrent <- merge(afCurrent, afDonorTotals, by.x = "accountID", by.y = "accountID", all.x = TRUE)
View(afCurrent)

# Confirm no rows exist where donation total is NA:
amountNA <- which(is.na(afCurrent$donationTotal))
View(amountNA)

# Create new column showing donor levels:
afCurrent <- afCurrent %>%
  mutate(
    level = case_when(
      donationTotal<125 ~ "recent donors",
      donationTotal>=125 & donationTotal<250 ~ "Supporter-level donors",
      donationTotal>=250 & donationTotal<500 ~ "Investor-level donors",
      donationTotal>=500 & donationTotal<1000 ~ "Underwriter-level donors",
      donationTotal>=1000 & donationTotal<2500 ~ "Performer-level donors",
      donationTotal>=2500 & donationTotal<5000 ~ "Director-level donors",
      TRUE ~ "Producer-level donors"
    )
  )

#Create lastGiftSentence variable for the letter that is populated for subscribers who donated their refunds and for donors who
#have given since our closure.
afCurrent <- data.table(afCurrent)
afCurrent[, wordDate := format(lastGiftDate, format = "%B %d, %Y")]
afCurrent[, lastGiftDollars := dollar(afCurrent$lastGiftAmt)]

afCurrent <- afCurrent %>%
  mutate(
    lastGiftSentence = case_when(
      paymentType == "Ticket Order Refund" &
        donationRecordType == "PatronTicket Donation" &
        !is.na(amountDonated) &
        lastGiftDate >= "2020-03-01" &
        subscriber == 1 ~ "donating your subscription refund to RTP.)",
      subscriber == 1 &
        !is.na(amountDonated) &
        !is.na(amountCredited) ~"donating a portion of your subscription refund to RTP.)",
      subscriber == 1 &
        !is.na(amountDonated) &
        lastGiftAmt != amountDonated ~ paste0("both donating your subscription refund to RTP and contributing ",lastGiftDollars," to our annual fund on ",wordDate,".)"),
      paymentType == "Ticket Order Refund" &
        donationRecordType == "PatronTicket Donation" &
        lastGiftDate >= "2020-03-01" &
        subscriber == 0 ~ "donating your recent ticket refund to RTP's annual fund.)",
      paymentType == "Credit Card - Third Party" &
        donationRecordType == "Donation" &
        type == "GIVINGTUESDAYNOW" &
        lastGiftDate >= "2020-05-01" ~ paste("your recent donation of",lastGiftDollars,"to our GIVINGTUESDAYNOW Matching Fund Initiative.)"),
      TRUE ~ paste0("your most recent annual fund contribution of ",lastGiftDollars," on ",wordDate,".)")
    )
  )

# Find rows where street is blank:
streetNACur <- which(is.na(afCurrent$street))

# Remove the records with an @ in the Account Name or Address field:
which(str_detect(afCurrent$accountName, "@"))
which(str_detect(afCurrent$street, "@"))

#Handle records found above:
afCurrent$accountName[608] <- "Bonnie Fenton"
afCurrent$salutation[608] <- "Bonnie"
afCurrent$accountName[668] <- "Richard Bollana"

# Find the current donors that are marked "do Not Mail"
doNotMailCurrent <- afCurrent$doNotMail == "1"

# Save the records without do Not Mail marked over the data table called afDonors
afCurrent <- data.table(afCurrent)
afCurrent4DM <- afCurrent[doNotMail == "0",]

# Fix the row "names" (which are actually index numbers) to account for the two rows that were removed:
rownames(afDonors4DM) <-seq(length=nrow(afDonors4DM))

#Remove unncessary columns:
afCurrent4DM <- afCurrent4DM[, c(1, 5, 6, 9:14, 25, 30:35)]

# Save afDonorsDT to Excel as Currentdonors:
write_xlsx(afCurrent4DM, "/Users/georgesmith/Desktop/RTP/Season 28/Development/Spring Appeal 2020/AFMailing_spring2020_Currentdonors.xlsx", col_names = TRUE, format_headers = TRUE) 

# # # # # # LAPSED DONORS# # # # # # 
# Remove duplicates from the afLapsedDT data table:
uniqueafLapsedDT <- unique(afLapsedDT, by="accountID")
View(uniqueafLapsedDT)
# Save uniqueafLapsedDT back to afLapsedDT
afLapsedDT <- uniqueafLapsedDT
# Find the current donors that are marked "do Not Mail"
doNotMailLapsed <- afLapsedDT[do.not.mail == "1"]
# Save the records without do Not Mail marked over the data table called afDonorsDT
afLapsedDT <- afLapsedDT[do.not.mail != "1"]
# Create current donor list of businesses
afBusLapsedDT <- afLapsedDT[AccountRecordType == "Business"]
# Create current donor data table that includes on households and individuals:
afLapsedDT <- afLapsedDT[AccountRecordType %in% c("Household", "Individual")]
# Remove unnecessary columns from afLapsedDT:
afLapsedDTreduced <- afLapsedDT[, -c(3:4, 6:9, 16:18)]
View(afLapsedDTreduced)
afLapsedDT <- afLapsedDTreduced
# Which funds are reflected in this data table?
levels(factor(afLapsedDT$Fund))
# Filter our the rows where fund = "25TH"
WrongFundLapsed <- which(afLapsedDT$Fund == "25TH")
afLapsedDT <- afLapsedDT[-WrongFundLapsed,]
# Fix the row "names" (which are actually index numbers) to account for the three rows that were removed:
rownames(afLapsedDT) <-seq(length=nrow(afLapsedDT))
# Dedupe afLapsedDT against afDonorsDT using dplyr's anti_join function, which returns the rows of the
# first table where it cannot find a match in the second table
library(dplyr)
uniqueLapsedIDs <- anti_join(afLapsedDT, afDonorsDT, by = "accountID")
afLapsedDT <- uniqueLapsedIDs
# Add level column to populate with "donor" for drop-in in the appeal letter:
afLapsedDT <- afLapsedDT %>% mutate(
  Level = "a donor"
)
# Add salutations to afLaspedDT:
afLapsedDTS <- merge(afLapsedDT, salutationsDT, by.x = "accountID", by.y = "accountID", all.x = TRUE)
# Create salutations for records whose salutations fields are NA:
afLapsedDT <- afLapsedDTS %>% 
  mutate(
    LetterSalutation = ifelse(test = is.na(Salutation),
                              yes = AccountName,
                              no = Salutation)
  )
# Find and remove email addresses from AccountName:
library(stringr)
str_detect(afLapsedDT$AccountName, "@")
str_detect(afLapsedDT$MailingStreet, "@")
# Remove the record that contained this:
afLapsedDT <- afLapsedDT[-5,]
# Save afLapseddonorsDT to Excel as Lapseddonors:
write.xlsx(afLapsedDT, file = "AFMailing_Lapseddonors.xlsx", gridLines = TRUE) 
# Remove unnecessary columns from afBusLapsedDT:
afBusLapsedDTreduced <- afBusLapsedDT[, -c(3:4, 6:9, 16:18)]
View(afBusLapsedDTreduced)
afBusLapsedDT <- afBusLapsedDTreduced
# Which funds are reflected in this data table?
levels(factor(afBusLapsedDT$Fund))
# All funds for which there are donation here are for the Annual Fund so no filtering on fund is required here.
# Fix the row "names" (which are actually index numbers) to account for the rows that were removed:
rownames(afBusLapsedDT) <-seq(length=nrow(afBusLapsedDT))
# Dedupe afLapsedDT against afBusLapsedDT using dplyr's anti_join function, which returns the rows of the
# first table where it cannot find a match in the second table
uniqueBusLapsedIDs <- anti_join(afBusLapsedDT, afBusCurrentDT, by = "accountID") 
# No dupes across data tables so remove the afBusLapsedDT
rm(afBusLapsedDT, uniqueBusLapsedIDs)

# # # # # # Ticket Buyers 7/1/17 - 4/24/19# # # # # # 
# Remove duplicates from the afTicketsDT data table (based on Conact ID):
uniqueAFTicketsDT <- afTicketsDT %>% distinct(
  contactID, .keep_all = TRUE
)
View(uniqueafTicketsDT)
# Remove duplicates from the afTicketsDT data table (based on Account):
uniqueAFAcctTicketsDT <- afTicketsDT %>% distinct(
  account, .keep_all = TRUE
)
View(uniqueAFacctTicketsDT)
# Save uniqueafTicketDT back to afTicketDT since Account has more empty fields than Contact ID
afTicketsDT <- uniqueafTicketsDT
# Remove the two objects holding the unique records:
rm(uniqueAFacctTicketsDT, uniqueafTicketsDT)
# What are the possible values in doNotMail?
levels(factor(afTicketsDT$doNotMail))
# Find the ticket buyers that are marked "doNotMail"
doNotMailTickets <- afTicketsDT[doNotMail == "1"]
# Find ticket buyers without do Not Mail marked over the data table called afTicketsDT
afTicketsMailOKDT <- afTicketsDT[doNotMail != "1"]
# Save the records without do Not Mail marked over the data table called afTicketsDT
afTicketsDT <- afTicketsMailOKDT
rm(afTicketsMailOKDT)
# Remove unnecessary columns from afTicketsDT:
afTicketsDTreduced <- afTicketsDT[, -c(2:3, 14:16, 18:20)]
View(afTicketsDTreduced)
afTicketsDT <- afTicketsDTreduced
rm(afTicketsDTreduced)
# Which ticket buyer types are reflected in this data table?
levels(factor(afTicketsDT$TicketOrderType))
# Use previously imported and joined donor object (from other script file) to add contactID to the afDonorsDT 
# and afLapsedDT objects, which will then allow for duplicate removal across afTicketsDT
# First, name donors a data table
DTdonors <- data.table(donors)
uniqueDTdonors <- unique(DTdonors, by = "Account.ID")
# double check number of dupes to ensure the previous action yieled the correct result
dupedonorsDT <- DTdonors[duplicated(DTdonors$Account.ID),]
# Save unique records over DTdonors
DTdonors <- uniqueDTdonors
rm(uniqueDTdonors)
# Remove all columns except Account.ID (1), Contact.ID(4) and Account.Name(9)
reducedDTdonors <- DTdonors[,-c(2:3, 5:8, 10:49)]
DTdonors <-reducedDTdonors
rm(reducedDTdonors)
# Join DTdonors with afDonorsDT and afLapsedDT
afDonorsCidDT <- merge(afDonorsDT, DTdonors, by.x = "accountID", by.y = "Account.ID")
afLapsedCidDT <- merge(afLapsedDT, DTdonors, by.x = "accountID", by.y = "Account.ID")
# Rename contactID (in afTicketsDT) to Contact.ID to match the field name in the two objects above
names(afTicketsDT)[names(afTicketsDT) == "contactID"] <- "Contact.ID"
# Dedupe afTicketsDT against afDonorsCidDT using dplyr's anti_join function, which returns the rows of the
# first table where it cannot find a match in the second table
notCurrentDT <- anti_join(afTicketsDT, afDonorsCidDT, by = "Contact.ID")
ticketBuyersDT <- anti_join(notCurrentDT, afLapsedCidDT, by = "Contact.ID")
# Dedupe again: this time, use Account Name
notCurrentDT <- anti_join(afTicketsDT, afDonorsCidDT, by = c("Account" = "AccountName"))
ticketBuyersDT <- anti_join(notCurrentDT, afLapsedCidDT, by = c("Account" = "AccountName"))
# Add address fields to ticketBuyersDT:
accountsDT <- data.table(accounts)
ticketBuyersMailDT <- inner_join(ticketBuyersDT, accountsDT, by = "Account")
ticketBuyersNoAddr <- anti_join(ticketBuyersDT, ticketBuyersMailDT, by = "Account")
# Dedupe again; now on account id
notCurrentDT <- anti_join(ticketBuyersMailDT, afDonorsCidDT, by = "accountID")
afticketBuyersMailDT <- anti_join(ticketBuyersMailDT, afLapsedCidDT, by = "accountID")
# Remove some objects we no longer need
rm(notCurrentDT, afLapsedCidDT, afDonorsCidDT)
# Classify each ticket buyer as "loyal subscriber" or "someone who appreciates our productions"
# First, we need to see which values are present in the field (TicketOrderType) we'll query to create the new field:
levels(factor(afticketBuyersMailDT$TicketOrderType))
# Now, we'll create a new field called "Level" (to match the field name in the other data tables) with the ticket buyer class noted:
afticketBuyersMailDT <- afticketBuyersMailDT %>% 
  mutate(
  Level = case_when(
    TicketOrderType == "Tickets; Subscription" ~ "a loyal Subscriber",
    TRUE ~ "someone who appreciates our productions"
  )
)
# Add salutations to afticketBuyersMailDT:
afticketBuyersMailDT <- merge(afticketBuyersMailDT, salutationsDT, by.x = "accountID", by.y = "accountID", all.x = TRUE)
# Create salutations for records whose salutations fields are NA:
afticketBuyersMailDT <- afticketBuyersMailDT %>% 
  mutate(
    LetterSalutation = case_when(
      Account.Record.Type == "Individual" ~ firstName,
      TRUE ~ Account
    )
  )
afticketBuyersMailDT <- afticketBuyersMailDT %>% 
  mutate(LetterSalutation = ifelse(test = (accountRecordType == "Individual" & is.na(ContactInformalSalutation)),
                                   yes = firstName,
                                   no = ifelse(test = accountRecordType == "Individual",
                                               yes = ContactInformalSalutation,
                                               no = account))
  )
  

# Remove the records with an @ in the Account Name or Address field:
which(str_detect(afticketBuyersMailDT$Account, "@"))
which(str_detect(afticketBuyersMailDT$MailingStreet, "@"))
afticketBuyersMailDT <- afticketBuyersMailDT[-c(326, 333, 350, 358, 1715, 1716, 1717, 1718, 1722, 1723, 1741, 1742),]
# Save ticketBuyersDT to excel:
write.xlsx(afticketBuyersMailDT, file = "AFMailing_ticketBuyers.xlsx", gridLines = TRUE) 
# # # # NOTE:No subscribers were found in the ticket buyer only list (AKA all current subscribers are donors)
# Fix the row "names" (which are actually index numbers) to account for any rows that were removed:
rownames(ticketBuyersDT) <-seq(length=nrow(ticketBuyersDT))
# Append afDonorsDT to afLapsedDT:
# First, must make all columns match between the data.tables:
afLapsedDT <- afLapsedDT %>%
  mutate(
    donationTotal = "0"
    )
afLapsedDT$donationTotal <- as.numeric(as.character(afLapsedDT$donationTotal))
afLapsedDTtest <- afLapsedDT
# Now, append
donorUnion <- union(afDonorsDT, afLapsedDTtest)
# Create new object to hold these:
afAlldonorsDT <- donorUnion
rm(donorUnion)
# Save as Excel file:
write.xlsx(afAlldonorsDT, file = "AFMailing_donorList_Comprehensive.xlsx", gridLines = TRUE) 
