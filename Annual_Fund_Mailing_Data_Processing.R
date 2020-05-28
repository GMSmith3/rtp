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
afDonors <- fread("AFDonations_Jul2017_May252020.csv")
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

# Convert colnames in afTickets dt to camel case to match those in other dts
colnames(afTickets) <- c("contactID", "accountName", "contactName", "firstName", "lastName", "status", "donationName",
                         "amount", "street", "city", "state", "zip", "subcriptionStatus", "createdDate", "priceLevelName")

# Create donor datatable that includes only households and individuals:
afDonors <- afDonors[accountRecordType %in% c("Household", "Individual")]

# Remove rows where either amount or lastDonationAmtPM is na (they mess up later processing)
afDonors <- afDonors[!is.na(amount)]
#afDonors <- afDonors[!is.na(lastDonationAmtPM)]
# Convert date columns to Date datatype to allow for filtering on this feature:
afDonors$closeDate <- as.Date(afDonors$closeDate, format = "%m/%d/%y")
afDonors$lastDonationDatePM <- as.Date(afDonors$lastDonationDatePM, format = "%m/%d/%y")
afTickets$createdDate <- as.Date(afTickets$createdDate, format = "%m/%d/%y")

# Append salutation field to afDonors
afDonors <- merge(afDonors, curFall19Salutations, by.x = "accountID", by.y = "accountID", all.x = TRUE)
View(afDonors)

#Add donor level column to this subset:
afDonors <- afDonors %>% 
  mutate(
    letterSalutation = ifelse(test = !(is.na(salutation)),
                              yes = salutation,
                              no = ifelse(test = informalSalutation != "",
                                          yes = informalSalutation,
                                          no = "$"))
  )

#Add subscriber column to subscribersDonateOrCredit to allow logic on that field after merge below
subscribersDonateOrCredit$subscriber <- 1

#Append amountDonated and amountCredited fields from subscriber datatable to afDonors:
subscriberCols <- subscribersDonateOrCredit[, 11:14]
afDonors <- merge(afDonors, subscriberCols, by.x = "accountID", by.y = "accountID", all.x = TRUE)

#Change nulls in subscriber column to 0
afDonors$subscriber[is.na(afDonors$subscriber)] <- 0

# Break out donations from afDonors where closeDate < 07/01/2018, creating lapsed dt:
afDonors <- data.table(afDonors) #reverting back to datatable to enable following filtering methods
afLapsed <- afDonors[closeDate < "2018-07-01"][order(accountID, closeDate)] # sorting later simplifies selecting latest donation row for each accountID
afCurrent <- afDonors[closeDate > "2018-06-30"][order(accountID, closeDate)] # sorting later simplifies selecting latest donation row for each accountID

# # # # # # CURRENT DONORS (including CURRENT DONOR SUBSCRIBERS)# # # # # # 

# Create lastGiftByDonor datatable, containing only accountID and two renamed cols lastGiftDate and lastGiftAmt column, grouped by accountID for afCurrent
lastGiftByCurDonor <- afCurrent[
  order(-closeDate), head(.SD, 1), by = accountID][, 1:8][, .(accountID, lastGiftDate = closeDate, lastGiftAmt = amount)]

# Calculate the sum of amount column for every unique accountID. Call the new dataset - donationTotalDT:
# Add na.rm = TRUE to the sum function to avoid sum being reported as NA for accounts with an NA donation amount
afDonorTotals <- afCurrent[, .(donationTotal = sum(amount, na.rm=TRUE)), by = accountID]
View(afDonorTotals)

# Remove duplicates from the afCurrent datatable - keeping the most recent donation - to prepare it to join with afDonorTotals
uniqueCurDonors <- unique(afCurrent, by="accountID", fromLast = TRUE)
View(uniqueCurDonors)

# Append lastGiftByCurDonor columns to the uniqueCurDonors datatable -- save it over afCurrent:
afCurrent <- merge(uniqueCurDonors, lastGiftByCurDonor, by.x = "accountID", by.y = "accountID", all.x = TRUE)

# Add donation totals to afCurrent dt by joining uniqueCurDonors and afDonorTotals on accountID
afCurrent <- merge(afCurrent, afDonorTotals, by.x = "accountID", by.y = "accountID", all.x = TRUE)
View(afCurrent)

# Confirm no rows exist where donation total is NA:
amountNA <- which(is.na(afCurrent[,donationTotal]))
View(amountNA)

# Create new column showing donor levels:
afCurrent <- afCurrent %>%
  mutate(
    donorLevel = case_when(
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
#afCurrent$letterSalutation[631] <- "Bonnie"

# Find the current donors that are marked "do Not Mail"
doNotMailCurrent <- afCurrent$doNotMail == "1"

# Save the records without do Not Mail marked over the data table called afDonors
afCurrent <- data.table(afCurrent)
afCurrent4DM <- afCurrent[doNotMail == "0",]

# Fix the row "names" (which are actually index numbers) to account for the two rows that were removed:
rownames(afCurrent4DM) <-seq(length=nrow(afCurrent4DM))

#Remove unncessary columns:
afCurrentDirectMail <- afCurrent4DM[, c(1, 2, 6, 9:14, 26, 33, 36)]

# Save afDonorsDT to Excel as Currentdonors:
write_xlsx(afCurrentDirectMail, "/Users/georgesmith/Desktop/RTP/Season 28/Development/Spring Appeal 2020/AFMailing_spring2020_Currentdonors_Final.xlsx", col_names = TRUE, format_headers = TRUE) 

# Create board of directors list from current donors list:
currentBoard <- afCurrentDirectMail[c(176, 193, 203, 232, 255, 257, 335, 417, 424, 426, 459, 461, 487, 508, 736)]

#Save currentBoard file:
write_xlsx(currentBoard, "/Users/georgesmith/Desktop/RTP/Season 28/Development/Spring Appeal 2020/AFMailing_spring2020_current_Board_suppression_list.xlsx", col_names = TRUE, format_headers = TRUE) 

#Read in current donors without addresses:
noAddrCurDonors <- fread("naAddrCurrents.csv")

#Add addresses to current donors records lacking them
afCurrentAddr <- merge(noAddrCurDonors, accounts, by.x = "accountID", by.y = "accountID", all.x = TRUE)

# Remove duplicates from the afCurrentAddr datatable - keeping the most recent donation:
afCurrentAddrUnique <- unique(afCurrentAddr, by="accountID", fromLast = TRUE)

# Remove unnecessary columns
afCurrentAddrUnique <- afCurrentAddrUnique[, c(1:5, 10:12, 15, 17:22)] #remove columns that won't be needed 

# Save afCurrentAddrUnique to Excel as CurrentdonorsAddrAdded:
write_xlsx(afCurrentAddrUnique, "/Users/georgesmith/Desktop/RTP/Season 28/Development/Spring Appeal 2020/AFMailing_spring2020_CurrentdonorsAddrAdded.xlsx", col_names = TRUE, format_headers = TRUE) 

# # # # # # LAPSED DONORS# # # # # # 
# Create lastGiftByDonor datatable, containing only accountID and two renamed cols lastGiftDate and lastGiftAmt column, grouped by accountID for afLapsed
lastGiftByLapsedDonor <- afLapsed[
  order(-closeDate), head(.SD, 1), by = accountID][, 1:8][, .(accountID, lastGiftDate = closeDate, lastGiftAmt = amount)]

# Remove duplicates from the afLapsed datatable - keeping the most recent donation - to prepare remove donors in current dt:
uniqueLapsedDonors <- unique(afLapsed, by="accountID", fromLast = TRUE)
View(uniqueLapsedDonors)

#Find the rows in uniqueLapsedDonors that are not present in uniqueCurDonors based on accountID --> lapsed:
afLapsedNotInCurrent <- anti_join(uniqueLapsedDonors, uniqueCurDonors, by = "accountID")
View(afLapsedNotInCurrent)

# Append lastGiftByLapsedDonor columns to the afLapsedNotInCurrent datatable -- save it over afLapsed:
afLapsed <- merge(afLapsedNotInCurrent, lastGiftByLapsedDonor, by.x = "accountID", by.y = "accountID", all.x = TRUE)

# Find the current donors that are marked "do Not Mail"
afLapsed <- data.table(afLapsed)
doNotMailLapsed <- afLapsed[doNotMail == "1"]

# Save the records without do Not Mail marked over the data table called afDonorsDT
afLapsed <- afLapsed[doNotMail != "1"]

# Fix the row "names" (which are actually index numbers) to account for the two rows that were removed:
rownames(afLapsed) <-seq(length=nrow(afLapsed))

# Remove unnecessary columns from afLapsed:
afLapsedDirectMail <- afLapsed[, c(1, 2, 6, 9:14, 26)]
View(afLapsedDirectMail)

# Find and remove email addresses from AccountName:
which(str_detect(afLapsedDirectMail$accountName, "@"))
which(str_detect(afLapsedDirectMail$street, "@"))

#Handle records found above:
afLapsedDirectMail$accountName[17] <- "Kirsten McKinney"
afLapsedDirectMail$letterSalutation[17] <- "Kirsten"
afLapsedDirectMail$accountName[20] <- "Stephanie Smith"
afLapsedDirectMail$letterSalutation[20] <- "Stephanie"
afLapsedDirectMail$accountName[156] <- "Patrick Peters"
afLapsedDirectMail$letterSalutation[156] <- "Patrick"

# Save afDonorsDT to Excel as Currentdonors:
write_xlsx(afLapsedDirectMail, "/Users/georgesmith/Desktop/RTP/Season 28/Development/Spring Appeal 2020/AFMailing_spring2020_Lapseddonors_Final.xlsx", col_names = TRUE, format_headers = TRUE) 

# # # # # # Ticket Buyers Season 27# # # # # # 

#Add accountID to afTickets
afTickets <- afTickets[, c(1, 4, 5, 7, 8, 14, 15)] #remove columns that won't be needed after joining with accounts dt
afTicketsAccts <- merge(afTickets, accounts, by.x = "contactID", by.y = "contactID", all.x = TRUE)

#Remove rows where contactID is blank ("")
afTicketsAccts <- afTicketsAccts[contactID != ""]

#Remove rows where accountID is na
afTicketsAccts <- afTicketsAccts[!is.na(accountID)]

# Remove duplicates from the afCurrent datatable - keeping the most recent donation - to prepare it to join with afDonorTotals
uniqueTicketBuyers <- unique(afTicketsAccts, by="accountID", fromLast = TRUE)

#Find the rows in uniqueTicketBuyers that are not present in afCurrentDirectMail based on accountID --> lapsed:
afTBNotInCurrent <- anti_join(uniqueTicketBuyers, afCurrentDirectMail, by = "accountID")

#Find the rows in uniqueTicketBuyers that are not present in afLapsedDirectMail based on accountID --> lapsed:
afTBNotInCurrentOrLapsed <- anti_join(afTBNotInCurrent, afLapsedDirectMail, by = "accountID")

# Remove the records with an @ in the Account Name or Address field:
which(str_detect(afTBNotInCurrentOrLapsed$accountName, "@"))
which(str_detect(afTBNotInCurrentOrLapsed$street, "@"))

#Handle records found above:
afTBNotInCurrentOrLapsed$accountName[140] <- "Michael Clay"
afTBNotInCurrentOrLapsed$accountName[141] <- "Darlene Hicks"
afTBNotInCurrentOrLapsed$accountName[142] <- "Catherine Smith"
afTBNotInCurrentOrLapsed$accountName[143] <- "Kevin Clayton"
afTBNotInCurrentOrLapsed$accountName[787] <- "Robert Keyes"
afTBNotInCurrentOrLapsed$accountName[958] <- "Alfie Alfonso"
afTBNotInCurrentOrLapsed$accountName[959] <- "Freeman Turley"
afTBNotInCurrentOrLapsed$accountName[960] <- "William Shockley"
afTBNotInCurrentOrLapsed$accountName[961] <- "Derek Tate"
afTBNotInCurrentOrLapsed$accountName[962] <- "Kathy DeSantis"
afTBNotInCurrentOrLapsed$accountName[963] <- "Catherine Sirha"
  
# Save afDonorsDT to Excel as Currentdonors:
write_xlsx(afTBNotInCurrentOrLapsed, "/Users/georgesmith/Desktop/RTP/Season 28/Development/Spring Appeal 2020/AFMailing_spring2020_TicketBuyersS27_Final.xlsx", col_names = TRUE, format_headers = TRUE) 