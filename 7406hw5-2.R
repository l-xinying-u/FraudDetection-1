library(dplyr)
library(data.table)
library(readr)
library(stringr)


# Load transaction data
train_transaction <- fread("train_transaction.csv")


print(head(train_transaction, 3))

# Check fraud distribution in transaction data
if("isFraud" %in% names(train_transaction)) {
  cat("\n=== FRAUD DISTRIBUTION ===\n")
  fraud_summary <- train_transaction[, .(
    total = .N,
    fraud_cases = sum(isFraud),
    fraud_rate = round(mean(isFraud) * 100, 4)
  )]
  print(fraud_summary)
  
  cat("\nFraud count table:\n")
  print(table(train_transaction$isFraud))
}


# Remove columns with very high missing values for quick analysis
quick_analysis_data <- function(df, na_threshold = 0.8) {
  df_quick <- copy(df)
  
  # Calculate NA percentage for each column
  na_percentage <- sapply(df_quick, function(x) mean(is.na(x)))
  
  cat("Columns removed (NA >", na_threshold * 100, "%):\n")
  high_na_cols <- names(na_percentage)[na_percentage > na_threshold]
  print(high_na_cols)
  
  # Remove high-NA columns
  df_quick <- df_quick[, !..high_na_cols]
  
  cat("\nOriginal dimensions:", dim(df), "\n")
  cat("Quick analysis dimensions:", dim(df_quick), "\n")
  cat("Reduction:", round((1 - ncol(df_quick)/ncol(df)) * 100, 1), "% fewer columns\n")
  
  return(df_quick)
}

# Apply to your transaction data
train_quick <- quick_analysis_data(train_transaction)
