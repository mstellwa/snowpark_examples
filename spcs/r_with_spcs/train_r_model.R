library(tidyverse)
library(tidymodels)

setwd("~/Documents/GitHub/mstellwall-demos/spcs/simple_r_func")

churn <- read.csv('./data/Churn_Modelling.csv')
glimpse(churn)

head(churn)

# Remove columns not needed and fix the data types
churn_clean <-  churn %>% 
  select(-c(RowNumber, CustomerId, Surname )) %>%
  mutate_if(is.character, as.factor) %>% 
  mutate(
    HasCrCard = as.factor(HasCrCard),
    IsActiveMember = as.factor(IsActiveMember),
    Exited = as.factor(Exited),
    Exited = factor(Exited, levels = c(0, 1),
                    labels = c("Churn",
                               "Not Churn")))

glimpse(churn_clean)


churn_clean %>% is.na() %>% colSums()

set.seed(123)
churn_split <- initial_split(churn_clean,  strata = Exited)

churn_train <- training(churn_split)
churn_test  <- testing(churn_split)

# Create a Logistic Regression model using glm
log_reg_mod <- logistic_reg() %>% 
  set_engine("glm") %>% 
  set_mode("classification") 

# Train the model on our data
log_reg_fit <- log_reg_mod %>%
  fit(Exited ~ ., data = churn_train)

# Get information about the trained model
log_reg_fit
summary(log_reg_fit)

# Estimate performance of our model
# We will use the area under the Receiver Operating Characteristic (ROC) curve, and overall classification accuracy.
log_reg_testing_pred <- 
  predict(log_reg_fit, churn_test) %>% 
  bind_cols(predict(log_reg_fit, churn_test, type = "prob")) %>% 
  bind_cols(churn_test %>% select(Exited))

head(log_reg_testing_pred)

# Overall accuracy and precision of our model
class_metrics <- metric_set(accuracy, precision)
log_reg_testing_pred %>%                  
  class_metrics(truth = Exited, estimate=.pred_class)


#Save the fitted R Model file
setwd("~/Documents/GitHub/mstellwall-demos/spcs/simple_r_func")
#saveRDS(log_reg_fit, file = "log_reg_mod.rds")
saveRDS(log_reg_fit, file = "./model/log_reg_mod.rds")

