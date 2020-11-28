#App to upload annual fund donor .csv and assign stewardship donor levels by account

ui <- fluidPage(
  fileInput("upload", NULL, buttonLabel = "Upload...", multiple = TRUE),
  DTOutput("files"),
  DTOutput("head")
)
server <- function(input, output) {
  output$files <- renderDT({
    req(input$upload)
    Data <- fread(input$upload$datapath)
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
    donors <- donors[CloseDate >= "2019-07-01" & CloseDate <= "2020-09-15"]
    #Calculate the sum of Amount column for every group in AccountID column - (remove NAs from the calculations). Call the new data - DonationTotalsDT:
    DonationTotalsDT <- donors[,.(AccountName, InformalSalutation, MailingStreet, MailingCity, MailingState, MailingZip, DonationTotal = sum(Amount, na.rm = TRUE), Amount, PledgeAmount, PaymentSchedule, CloseDate, Fund, PaymentType, Type),by=AccountID]
    #Let's sort the list by AccountID to see how prevalent duplicate records are:
    SortByAcct <- DonationTotalsDT[order(AccountID),]
    #Remove the duplicate records from DonationTotalsDT based on Account ID (keep most recent row - with most recent donation):
    UniqueDT <- unique(DonationTotalsDT[order(CloseDate)], by="AccountID", fromLast = TRUE)
    #Let's filter the data table to include only records with DonationTotal >= 125:
    DonationTotal125DT <- UniqueDT[DonationTotal >= 125,]
    #Let's sort the list by AccountID to see how prevalent duplicate records are:
    SortByAcct <- DonationTotalsDT[order(AccountID),]
    #Remove the duplicate records from DonationTotalsDT based on Account ID (keep most recent row - with most recent donation):
    UniqueDT <- unique(DonationTotalsDT[order(CloseDate)], by="AccountID", fromLast = TRUE)
    #Let's filter the data table to include only records with DonationTotal >= 125:
    DonationTotal125DT <- UniqueDT[DonationTotal >= 125,]
    #Let's sort the data table in ascending order:
    Sorted <- DonationTotal125DT[order(DonationTotal),]
    
    Sorted[, DonorLevel := fifelse(DonationTotal<250,
                                   "Supporters",
                                   fifelse(DonationTotal>=250 & DonationTotal<500,
                                           "Investors",
                                           fifelse(DonationTotal>=500 & DonationTotal<1000,
                                                   "Underwriters",
                                                   fifelse(DonationTotal>=1000 & DonationTotal<2500,
                                                           "Performers",
                                                           fifelse(DonationTotal>=2500 & DonationTotal<5000,
                                                                   "Directors",
                                                                   "Producers")))))]
    
    #################################################
    ##Segment donors into stewardship program levels
    #################################################
    #Show me the records with donation totals between 125 and 249:
    Supporters <- Sorted[DonationTotal<250,]
    #Show me the records with donation totals between 250 and 499:
    Investors <- Sorted[DonationTotal>=250 & DonationTotal<500,]
    #Show me the records with donation totals between 500 and 999:
    Underwriters <- Sorted[DonationTotal>=500 & DonationTotal<1000,]
    #Show me the records with donation totals between 1000 and 2499:
    Performers <- Sorted[DonationTotal>=1000 & DonationTotal<2500,] 
    #Show me the records with donation totals between 2500 and 4999:
    Directors <- Sorted[DonationTotal>=2500 & DonationTotal<5000,]
    #Show me the records with donation totals 5000 and up:
    Producers <- Sorted[DonationTotal>=5000,]
    #Alphabetize donors by last name within each group (for playbill listing):
    Supporters <- Supporters %>%
      arrange(gsub(".*\\s", "", AccountName))
    Investors <- Investors %>% 
      arrange(gsub(".*\\s", "", AccountName))
    Underwriters <- Underwriters %>% 
      arrange(gsub(".*\\s", "", AccountName))
    Performers <- Performers %>% 
      arrange(gsub(".*\\s", "", AccountName))
    Directors <- Directors %>% 
      arrange(gsub(".*\\s", "", AccountName))
    Producers <- Producers %>% 
      arrange(gsub(".*\\s", "", AccountName))
    
    View(Sorted)
  })

}

shinyApp(ui = ui, server = server)