library(dplyr)
library(data.table)
library(readr)
library(stringr)


# Load transaction data
train_transaction <- fread("train_transaction.csv")
test_transaction <- fread("test_transaction.csv")

# Load identity data
train_identity <- fread("train_identity.csv")
test_identity <- fread("test_identity.csv")

# Check dimensions
cat("Train Transaction dimensions:", dim(train_transaction), "\n")
cat("Test Transaction dimensions:", dim(test_transaction), "\n")
cat("Train Identity dimensions:", dim(train_identity), "\n")
cat("Test Identity dimensions:", dim(test_identity), "\n")

# Check column names
cat("\nTrain Transaction columns:\n")
print(names(train_transaction))

cat("\nTrain Identity columns:\n")
print(names(train_identity))

# Check for common key (TransactionID)
cat("\nChecking TransactionID in both datasets:\n")
cat("TransactionID in train_transaction:", "TransactionID" %in% names(train_transaction), "\n")
cat("TransactionID in train_identity:", "TransactionID" %in% names(train_identity), "\n")

# Join train datasets
train_combined <- train_transaction %>%
  left_join(train_identity, by = "TransactionID")

# Join test datasets  
test_combined <- test_transaction %>%
  left_join(test_identity, by = "TransactionID")

# Check dimensions after joining
cat("Train combined dimensions:", dim(train_combined), "\n")
cat("Test combined dimensions:", dim(test_combined), "\n")

# Get comprehensive column information
get_column_info <- function(df, df_name) {
  cat("\n====", df_name, "Column Analysis ====\n")
  
  # Total columns
  cat("Total columns:", ncol(df), "\n")
  
  # Column names
  cat("Column names:\n")
  print(names(df))
  
  # Data types summary
  cat("\nData types summary:\n")
  print(table(sapply(df, class)))
  
  # Check for NA percentages
  na_percentage <- sapply(df, function(x) mean(is.na(x))) * 100
  cat("\nColumns with high NA percentage (>50%):\n")
  high_na_cols <- na_percentage[na_percentage > 50]
  if(length(high_na_cols) > 0) {
    print(sort(high_na_cols, decreasing = TRUE))
  } else {
    cat("No columns with >50% NA values\n")
  }
  
  return(na_percentage)
}

# Analyze both datasets
train_na <- get_column_info(train_combined, "Train Combined")
test_na <- get_column_info(test_combined, "Test Combined")

# Function to classify columns
classify_columns <- function(df) {
  col_types <- sapply(df, class)
  
  categorical_cols <- names(df)[col_types %in% c("character", "factor")]
  numerical_cols <- names(df)[col_types %in% c("numeric", "integer")]
  
  # Also check for binary/flag columns (columns with only 0,1,NA)
  binary_candidates <- sapply(df, function(x) {
    if(is.numeric(x)) {
      unique_vals <- na.omit(unique(x))
      all(unique_vals %in% c(0, 1))
    } else {
      FALSE
    }
  })
  binary_cols <- names(df)[binary_candidates]
  
  list(
    categorical = categorical_cols,
    numerical = numerical_cols,
    binary = binary_cols
  )
}

# Classify columns for both datasets
train_cols <- classify_columns(train_combined)
test_cols <- classify_columns(test_combined)

cat("\nTrain Categorical columns count:", length(train_cols$categorical), "\n")
cat("Train Numerical columns count:", length(train_cols$numerical), "\n")
cat("Train Binary columns count:", length(train_cols$binary), "\n")

# Compare column sets between train and test
common_cols <- intersect(names(train_combined), names(test_combined))
cat("\nCommon columns between train and test:", length(common_cols), "\n")

# Check memory usage
cat("Memory usage:\n")
cat("Train combined:", format(object.size(train_combined), units = "MB"), "\n")
cat("Test combined:", format(object.size(test_combined), units = "MB"), "\n")


cat("\n=== SUMMARY REPORT ===\n")
cat("Original train transaction rows:", nrow(train_transaction), "\n")
cat("Original train identity rows:", nrow(train_identity), "\n")
cat("Combined train rows:", nrow(train_combined), "\n")
cat("Percentage of transactions with identity info:", 
    round(nrow(train_identity)/nrow(train_transaction)*100, 2), "%\n")

cat("\nKey findings:\n")
cat("- TransactionID is the joining key\n")
cat("-", sum(train_na > 50), "columns in train have >50% missing values\n")
cat("-", sum(test_na > 50), "columns in test have >50% missing values\n")
cat("- Total features after joining:", ncol(train_combined), "\n")


# Option 1: View specific columns for better readability
cat("\n=== KEY COLUMNS - FIRST 10 ROWS ===\n")

# Select key columns to display
key_cols <- c("TransactionID", "TransactionDT", "ProductCD", "card1", "card2","card3", 
              "card4","card5", "card6", "P_emaildomain", "TransactionAmt")

if(all(key_cols %in% names(train_combined))) {
  cat("\nTrain Combined - Key columns:\n")
  print(train_combined[1:10, ..key_cols])
}

# Check for target variable (isFraud)
if("isFraud" %in% names(train_combined)) {
  cat("\nTarget variable 'isFraud' distribution:\n")
  print(table(train_combined$isFraud))
}

cat("=== FIRST 10 ROWS OF EACH DATASET ===\n")

cat("\n1. TRAIN TRANSACTION (first 10 rows):\n")
print(head(train_transaction, 3))
print(head(train_identity, 3))


# Check if isFraud column exists
if("isFraud" %in% names(train_transaction)) {
  cat("=== FRAUD COUNT IN TRAINING SET ===\n")
  
  # Simple count
  fraud_count <- sum(train_transaction$isFraud)
  total_count <- nrow(train_transaction)
  fraud_rate <- fraud_count / total_count * 100
  
  cat("Fraud cases:", fraud_count, "\n")
  cat("Total transactions:", total_count, "\n")
  cat("Fraud rate:", round(fraud_rate, 4), "%\n")
  
  # Table view
  cat("\nFraud distribution:\n")
  print(table(train_transaction$isFraud))
}
