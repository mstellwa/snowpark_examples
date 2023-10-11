#FROM rocker/r-ver:4.0.2
FROM rocker/r-ver:4.3
RUN apt-get update -qq && apt-get install -y \
      libssl-dev \
      libcurl4-gnutls-dev
RUN R -e "install.packages('plumber', dependencies=TRUE)"
RUN R -e "install.packages('readr', dependencies=TRUE)"
RUN R -e "install.packages('yaml', dependencies=TRUE)"
RUN R -e "install.packages('tidyverse', dependencies=TRUE)"
RUN R -e "install.packages('tidymodels', dependencies=TRUE)"
RUN R -e "install.packages('swagger', dependencies=TRUE)"
RUN R -e "install.packages('rapidoc', dependencies=TRUE)"
COPY / /
EXPOSE 8080
ENTRYPOINT ["Rscript", "plumber_churn.R"]
