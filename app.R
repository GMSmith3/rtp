# Load R packages
library(shiny)
library(DT)
library(ggplot2)
library(shinythemes)
library(data.table)
library(dplyr)
library(stringr)
library(lubridate)

#App to upload Annual Fund donor .csv and assign stewardship donor levels by account
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
      DTOutput("file")
    ),
    #Main panel for displaying outputs ---
    mainPanel(
      
      #Add Download Button ---
      downloadButton('downloadData', "Download as .csv"),
      hr(),
      
      #Output: processed data file ---
      DTOutput("donorLevels"),
      hr(),
      #Output: plot
      plotOutput('plot1'),
      hr(),
      #Output: plot2
      plotOutput('plot2')
    )
  )  
)
server <- function(input, output){
  stewPeriodInput <- reactive({
    Data <- input$upload
    req(Data)
    Data <- fread(Data$datapath)
    colnames(Data) <- c("accountID", "accountRecordType", "doNotMail", "donationRecordType", "informalSalutation",
                        "accountName", "amount", "closeDate", "emailOptOut",  "email", "street", "city", "state",
                        "zip", "fund", "frequency", "paymentType", "pledgeAmount", "paymentSchedule", "type", "stage",
                        "fiscalYear", "lastDonationAmtPM", "lastDonationDatePM")
    Data$closeDate<-as.Date(Data$closeDate, format = "%m/%d/%y")
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
    #Select only the records where fund = Annual Fund or is.na:
    Data <- Data[fund =="Annual Fund" | fund == ""]
    Data[fund == "", fund := "Annual Fund"]
    
    #Filter out rows based on values in specific columns to get subset on which I'm ready to calculate donation metrics
    #and donor stewardship levels.
    donors <- Data[((accountRecordType %in% c("Household", "Individual")) &
                      (donationRecordType %in% c("Donation", "PatronTicket Donation", "Matching", "Pledge Payment")) &
                      (stage != "Refunded")),]
    
    #Filter for giving period (if not imported that way):
    donors[(closeDate >= input$date1CurrPer) & (closeDate <= input$date2CurrPer),
           currentStewardshipTotal := sum(amount, na.rm=TRUE), by = accountID][
             (closeDate >= as.Date(ymd(input$date1CurrPer) - years(1))) & (closeDate <= as.Date((ymd(input$date1CurrPer) - days(1)) + years(1))),
             prevStewardshipTotal := sum(amount, na.rm=TRUE), by = accountID]
    #Select columns to return & sort by accountID then closeDate:
    #donors[,.(accountName, InformalSalutation, MailingStreet, MailingCity, MailingState, MailingZip, DonationTotal = sum(amount, na.rm = TRUE), closeDate),by=accountID]
    donors <- donors[,.(accountID, accountName, amount, closeDate, fiscalYear, prevStewardshipTotal, currentStewardshipTotal)][order(accountID, closeDate)]
    
    return(donors)
  })
  
  levelsAssigned <- reactive({
    donors <- stewPeriodInput()
    #Remove the duplicate records from DonationTotalsDT based on account ID (keep most recent row - with most recent donation):
    #uniqueDT <- unique(donors, by="accountID", fromLast = TRUE)
    #Let's filter the data table to include only records with DonationTotal >= 125:
    #curStewards <- uniqueDT[currentStewardshipTotal >= 125,]
    curStewards <- donors[currentStewardshipTotal >= 125,]
    #Let's sort the data table in ascending order:
    #sorted <- curStewards[order(currentStewardshipTotal),]
    sorted <- curStewards[order(currentStewardshipTotal, accountID),]
    
    sorted[, DonorLevel := fifelse(currentStewardshipTotal<250,
                                   "Supporters",
                                   fifelse(currentStewardshipTotal>=250 & currentStewardshipTotal<500,
                                           "Investors",
                                           fifelse(currentStewardshipTotal>=500 & currentStewardshipTotal<1000,
                                                   "Underwriters",
                                                   fifelse(currentStewardshipTotal>=1000 & currentStewardshipTotal<2500,
                                                           "Performers",
                                                           fifelse(currentStewardshipTotal>=2500 & currentStewardshipTotal<5000,
                                                                   "Directors",
                                                                   "Producers")))))]
    return(sorted)
  })
  
  levelDTs <- reactive({
    data <- levelsAssigned()
    level <- input$subsets
    #Add medianDonation and donationCount columns to the datatable
    data[, `:=` (donationCount = .N, medianDonation = round(median(amount, na.rm = FALSE), 2)), by=accountID]
    
    data <- unique(sorted, by="accountID", fromLast = FALSE)[, .(accountID, accountName, donationCount, medianDonation, prevStewardshipTotal, currentStewardshipTotal, DonorLevel)]
    
    #Segment donors by level and alphabetization by last name
    Supporters <- sorted_unique[currentStewardshipTotal<250,] %>%
      arrange(gsub(".*\\s", "", accountName))
    Investors <- sorted_unique[currentStewardshipTotal>=250 & currentStewardshipTotal<500,] %>%
      arrange(gsub(".*\\s", "", accountName))
    Underwriters <- sorted_unique[currentStewardshipTotal>=500 & currentStewardshipTotal<1000,] %>%
      arrange(gsub(".*\\s", "", accountName))
    Performers <- sorted_unique[currentStewardshipTotal>=1000 & currentStewardshipTotal<2500,] %>%
      arrange(gsub(".*\\s", "", accountName)) 
    Directors <- sorted_unique[currentStewardshipTotal>=2500 & currentStewardshipTotal<5000,] %>%
      arrange(gsub(".*\\s", "", accountName))
    Producers <- sorted_unique[currentStewardshipTotal>=5000,] %>%
      arrange(gsub(".*\\s", "", accountName))
    
    if (level == "Supporters") {
      return(Supporters)
    } else if (level == "Investors") {
      return(Investors)
    } else if (level == "Underwriters") {
      return(Underwriters)
    } else if (level == "Performers") {
      return(Performers)
    } else if (level == "Directors") {
      return(Directors)
    } else if (level == "Producers") {
      return(Producers)
    } else {
      return(rbind(Supporters, Investors, Underwriters, Performers, Directors, Producers))
    }
  })
  
  levelViz <- reactive({
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
  
  output$plot1 <- renderPlot({
    donations <- levelViz()
    
    ggplot(data = donations[!(accountName %in% c('Barbara and James McCarthy','Wells Fargo Community Support Campaign')),], aes(x = closeDate, y = amount, size = amount, color = DonorLevel))+
      geom_point(stat='identity') +
      scale_x_date(guide = guide_axis(angle = 45),date_breaks = '1 month',  date_labels = '%B/%Y') +
      ggtitle("Annual Fund Giving", subtitle = "July 1, 2018 - September 9, 2020") +
      xlab("Time") + ylab("Amount($)") +
      theme_bw() + theme(legend.position = "left") +
      theme(plot.title = element_text(hjust = 0.5)) +
      theme(plot.subtitle = element_text(hjust = 0.5)) +
      guides(size = F)
  })
  
  output$plot2 <- renderPlot({
    data <- levelViz()
    
    ggplot(data = data[!(accountName %in% c('Barbara and James McCarthy','Wells Fargo Community Support Campaign')),], aes(x = accountName, y = amount, fill= fiscalYear))+
      geom_bar(stat='sum', position = 'dodge', color = 'black', size = .25) +
      scale_x_discrete(guide = guide_axis(angle = 45)) +
      ggtitle("Annual Fund Giving", subtitle = "July 1, 2018 - November 14, 2020") +
      xlab("Donor Account Name") + ylab("Period Donation Total") +
      theme_bw() + theme(legend.position = "left") +
      theme(plot.title = element_text(hjust = 0.5)) +
      theme(plot.subtitle = element_text(hjust = 0.5)) +
      guides(size = F)
    
  })
  
  output$donorLevels <- renderDT({
    levelDTs()
  })
  output$downloadData <- downloadHandler(
    filename = function() {
      paste0(input$subsets, '_', Sys.Date(), '.csv')
    },
    content = function(file) {
      write.csv(levelDTs(), file, row.names = FALSE)
    }
  )
}


shinyApp(ui = ui, server = server)