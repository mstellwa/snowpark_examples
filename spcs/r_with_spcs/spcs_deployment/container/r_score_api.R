library(plumber)
library(yaml)
library(tidyverse)
library(tidymodels)

options(warn=-1)

# Load the model
model <- readr::read_rds("/r-models/log_reg_mod.rds")

#* Test connection
#* @get /connection-status

function(){
  list(status = "Connection to R Churn Prediction API successful", 
       time = Sys.time())
}

#* Predict churn
#* @serializer unboxedJSON
#* @post /predict

function(req, res){
  #Input data comes as:
  #   {"data": [[row_nbr, value1, value2..], [row_nbr, value1, value2..]]  }
  #  
  input <- as.data.frame(req$body$data)
  # Input data has no columns name  
  colnames(input) <- c('row_nbr', 'CreditScore', 'Geography', 'Gender', 'Age', 'Tenure', 'Balance', 'NumOfProducts', 'HasCrCard', 'IsActiveMember', 'EstimatedSalary')
  
  # All data types is strings so we need to fix that
  input_clean <- input %>%
                    mutate(across( c(CreditScore, Age, Tenure, NumOfProducts), as.integer)) %>%
                      mutate(across(c(Balance, EstimatedSalary), as.numeric)) %>%
                        mutate_if(is.character, as.factor)
  
  pred <- predict(model, input_clean, type = "prob")
  #. Return need to be:
  #   {"data": [[row_nbr, value1, value2..], [row_nbr, value1, value2..]]  }
  #
  l <- split(pred, rownames(pred))
  returnData <- list(data = mapply(function(x, y) list(x - 1, as.list(y)), seq(length(l)), l, SIMPLIFY = FALSE))
  return(returnData)
}

#* @plumber
function(pr){
  pr %>% 
    pr_set_api_spec(yaml::read_yaml("openapi.yaml"))
  
}

