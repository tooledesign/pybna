
library(RPostgreSQL)
library(getPass)
library(reshape)
library(psych)
library(ggplot2)
library(openxlsx)
pgdrv <- dbDriver(drvName = "PostgreSQL")
db <-DBI::dbConnect(pgdrv,
                    dbname="bna",
                    host="192.168.60.220", port=5432,
                    user = 'gis',
                    password = getPass("Enter Password:gis"))
website_scores <- DBI::dbGetQuery(db,"SELECT * FROM website_scores;")
new_scores <- DBI::dbGetQuery(db,"SELECT * FROM bna_score_destinations_second;")

setwd("C:\\Users\\dpatterson\\code\\pybna\\pybna\\testing\\pybna_comparison")

################
##Website Data
################

# get basic summary stats
website_scores_descriptives <- describe(website_scores[c(-1, -48)], na.rm=TRUE)
website_scores_descriptives <- data.frame("attribute"=rownames(website_scores_descriptives), website_scores_descriptives)

#count nulls
website_scores_nas <-sapply(website_scores, function(y) sum(length(which(is.na(y)))))
website_scores_nas <- data.frame(website_scores_nas)
website_scores_not_nas <-sapply(website_scores, function(y) sum(length(which(!is.na(y)))))
website_scores_not_nas <- data.frame(website_scores_not_nas)
website_scores_nas_final <- cbind(website_scores_nas, website_scores_not_nas)
website_scores_nas_final$total <- website_scores_nas_final$website_scores_nas + website_scores_nas_final$website_scores_not_nas
website_scores_nas_final$na_percent <- round(website_scores_nas_final$website_scores_nas*100/website_scores_nas_final$total, 2)
website_scores_nas_final$not_na_percent <- round(website_scores_nas_final$website_scores_not_nas*100/website_scores_nas_final$total, 2)
website_scores_nas_final <- data.frame("attribute"=rownames(website_scores_nas_final), website_scores_nas_final)
nas_in_website_scores <- website_scores_nas_final


################
## New Scores
################

# get basic summary stats
new_scores_descriptives <- describe(new_scores[c(-1, -48)], na.rm=TRUE)
new_scores_descriptives <- data.frame("attribute"=rownames(new_scores_descriptives), new_scores_descriptives)

#count nulls
new_scores_nas <-sapply(new_scores, function(y) sum(length(which(is.na(y)))))
new_scores_nas <- data.frame(new_scores_nas)
new_scores_not_nas <-sapply(new_scores, function(y) sum(length(which(!is.na(y)))))
new_scores_not_nas <- data.frame(new_scores_not_nas)
new_scores_nas_final <- cbind(new_scores_nas, new_scores_not_nas)
new_scores_nas_final$total <- new_scores_nas_final$new_scores_nas + new_scores_nas_final$new_scores_not_nas
new_scores_nas_final$na_percent <- round(new_scores_nas_final$new_scores_nas*100/new_scores_nas_final$total, 2)
new_scores_nas_final$not_na_percent <- round(new_scores_nas_final$new_scores_not_nas*100/new_scores_nas_final$total, 2)
new_scores_nas_final <- data.frame("attribute"=rownames(new_scores_nas_final), new_scores_nas_final)
nas_in_new_scores <- new_scores_nas_final


#grab the min, median, means, max
website_mmmm <- website_scores_descriptives[c(1, 9, 6, 4, 10)]
website_mmmm <- website_mmmm[website_mmmm$attribute %like% "score", ]
new_mmmm <- new_scores_descriptives[c(1, 9, 6, 4, 10)]
new_mmmm <- new_mmmm[new_mmmm$attribute %like% "score", ]
mmmm <- merge(website_mmmm, new_mmmm, by='attribute', suffixes = c("_webs_sc","_pybna_sc"))

#means
website_means <- website_scores_descriptives[c(1, 4)]
website_means <- website_means[website_means$attribute %like% "score", ]
new_means <- new_scores_descriptives[c(1, 4)]
new_means <- new_means[new_means$attribute %like% "score",]
means <- merge(website_means, new_means, by='attribute', suffixes =c("_web_sc", '_pybna_sc'))
means$difference <- round(abs(means$mean_pybna_sc - means$mean_web_sc), 2)
means$direction<- ifelse(means[3]-means[2] >0, 'higher', 'lower')

# write files for review
xlsx_file <- "C:\\Users\\dpatterson\\code\\pybna\\pybna\\testing\\pybna_comparison\\pybna_comparison_statistics.xlsx"
list_of_sheets <- list(website_scores_descriptives, new_scores_descriptives, nas_in_website_scores, nas_in_new_scores, means, mmmm, website_scores[-48], new_scores[-48])
#remove file if exists
file_exists <- file.exists(xlsx_file)
if (isTRUE(file_exists)){
  file.remove(xlsx_file)
} else {
  print("File does not exist.")
}

write.xlsx(
  x = list_of_sheets,
  file = xlsx_file
)

wb <- openXL(xlsx_file)
#manually update column names...
#website_sc_desc
#pybna_sc_desc
#website_nulls
#pybna_score_nulls
#mean_score_comparison
#min_mean_median_max_scores
#website_raw
#pybna_raw
##################
##  Make Plots  ##
##################
#make long format
melt_scores <- melt(website_scores[,-c(1)])
#create hists on variable column
png("C:\\Users\\dpatterson\\code\\pybna\\pybna\\testing\\pybna_comparison\\website_scores_plots.png", width = 1000, height = 1000)
ggplot(melt_scores,aes(x = value)) +
  facet_wrap(~variable,scales = "free_x") + #scales 'free_x' because ranges are different
  geom_histogram(ylab = 'blockid count') +
  labs(title="Histograms of Website Scores", x="Value", y="BlockID Count")
dev.off()

#repeat with new data
png("C:\\Users\\dpatterson\\code\\pybna\\pybna\\testing\\pybna_comparison\\new_scores_plots.png", width = 1000, height = 1000)
new_plots <- melt_scores_new <- melt(new_scores[,-c(1)])
ggplot(melt_scores_new,aes(x = value)) +
  facet_wrap(~variable,scales = "free_x") +
  geom_histogram() +
  labs(title="Histograms of PyBNA Scores on Website Data", x="Value", y="BlockID Count")
dev.off();
