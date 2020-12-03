# Load R packages
library(shiny)
library(shinythemes)
library(data.table)
library(dplyr)
library(stringr)

#App to upload annual fund donor .csv and assign stewardship donor levels by account
ui <- fluidPage(
  # App title ----
  titlePanel("RTP Stewardship Level Assignment Tool"),
  
  # Sidebar layout with input and output definitions ----
  sidebarLayout(
    
    # Sidebar panel for inputs ----
    sidebarPanel(
      # Copy the line below to make a file upload manager
      fileInput(inputId = "upload", 
                label = h3("Select Donor Data File"), 
                accept = c('text/csv',
                           "text/comma-separated-values,text/plain",
                           ".csv"),
                buttonLabel = "Browse Files",
                placeholder = "Awaiting .csv Selection"),
      hr(),
      dateInput('date1CurrPer',
                label = 'Start Date of Current Stewardship Period',
                value = as.Date('2019-07-01')
      ),
      
      dateInput('date2CurrPer',
                label = paste('End Date of Current Stewardship Period Analysis'),
                value = Sys.Date(),
                format = "yyyy-mm-dd",
                startview = 'month', weekstart = 0
      ),
      hr(),
      selectInput("subsets",
                  label = h3("Select Stewardship Level to View:"),
                  choices = c("All Donor Levels" = "All Donor Levels",
                              "Supporters" = "Supporters",
                              "Investors" = "Investors",
                              "Underwriters" = "Underwriters",
                              "Performers" = "Performers",
                              "Directors" = "Directors",
                              "Producers" = "Producers"),
                  selected = 1,
                  multiple = FALSE),
      hr(),
      #Output: Data file ---
      DTOutput("file"),
    ),
    #Main panel for displaying outputs ---
    mainPanel(
      
      #Add Download Button ---
      downloadButton('downloadData', "Download"),
      hr(),
      
      #Output: processed data file ---
      DTOutput("donorLevels")
    )
  )  
)
server <- function(input, output){
  stewPeriodInput <- reactive({
    Data <- input$upload
    req(Data)
    Data <- fread(Data$datapath)
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
    Data[Fund == "", Fund := "Annual Fund"]
    
    #Filter out rows based on values in specific columns to get subset on which I'm ready to calculate donation metrics
    #and donor stewardship levels.
    donors <- Data[((AccountRecordType %in% c("Household", "Individual")) &
                      (DonationRecordType %in% c("Donation", "PatronTicket Donation", "Matching", "Pledge Payment")) &
                      (Stage != "Refunded")),]
    
    return(donors)
  })
  levelsAssigned <- reactive({
    donors <- stewPeriodInput()
    #Filter for giving period (if not imported that way):
    donors <- donors[CloseDate >= input$date1CurrPer & CloseDate <= input$date2CurrPer]
    #Calculate the sum of Amount column for every group in AccountID column - (remove NAs from the calculations). Call the new data - DonationTotalsDT:
    DonationTotalsDT <- donors[,.(AccountName, InformalSalutation, MailingStreet, MailingCity, MailingState, MailingZip, DonationTotal = sum(Amount, na.rm = TRUE), CloseDate),by=AccountID]
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
    return(Sorted)
  })
  levelDTs <- reactive({
    data <- levelsAssigned()
    level <- input$subsets
    
    if (level == "Supporters") {
      return(data[data$DonorLevel == "Supporters",])
    } else if (level == "Investors") {
      return(data[data$DonorLevel == "Investors",])
    } else if (level == "Underwriters") {
      return(data[data$DonorLevel == "Underwriters",])
    } else if (level == "Performers") {
      return(data[data$DonorLevel == "Performers",])
    } else if (level == "Directors") {
      return(data[data$DonorLevel == "Directors",])
    } else if (level == "Producers") {
      return(data[data$DonorLevel == "Producers",])
    } else {
      return(data)
    }
  })
  output$donorLevels <- renderDT({
    levelDTs()
  })
  output$downloadData <- downloadHandler(
    filename = function() {
      paste(input$subsets, input$filetype, sep=".")
    },
    content = function(file) {
      write.csv(levelDTs(), file, row.names = FALSE)
    }
  )
}


shinyApp(ui = ui, server = server)
