library(httr2)
library(knitr)
library(tidyverse)

# start date of CZNet project
date_start <- '2020-01-01'
# insert end date of reporting period
date_end <- '2025-05-31'

# input file containing access level of datasets deposited into external repos
cznet_datasets_external_csv <- 'in/unknown_access_datasets.csv'
# input file containing private and non-private datasets from Jenkins job (this is gitignored)
cznet_datasets_jenkins_csv <- 'in/jenkins_submission_report.csv'

# pull datasets from public discovery endpoint: https://github.com/cznethub/dsp-reports
cznet_datasets_df <- request(
  'https://dsp-reports-jbzfw6l52q-uc.a.run.app/csv') %>%
  # the request can take a minute or two, retries set arbitrarily 
  req_retry(max_tries = 5) %>%
  req_timeout(seconds = 120) %>%
  # perform request and interpret response as text 
  req_perform() %>%
  resp_body_string() %>%
  # read in csv, skip first column (index)
  read_csv(col_types = 
             cols(...1 = col_skip())) %>%
  # convert date columns
  mutate(datePublished=as.Date(datePublished),
         dateCreated=as.Date(dateCreated)) %>%
  # filter datasets after project start (after Jan 1 2020; before this is from CZO project)
  filter(dateCreated>=as.Date(date_start)) %>%
  # explicitly remove CZO or Hub affilated datasets
  filter(!str_detect(clusters, 'CZO|CZNet Hub')) %>%
  # remove json string elements
  mutate(across(everything(), ~gsub("\\[|'|\\]", "", .))) %>%
  # replace empty strings with na
  mutate(across(everything(), ~na_if(., ""))) %>%
  # remove datasets with no cluster information (~8%)
  drop_na(clusters) %>%
  # change NA access status to unknown (this is the case for Zenodo datasets)
  mutate(access = replace_na(access, "UNKNOWN"))


# inspect datasets outside of HydroShare and EarthChem for access level
cznet_datasets_reporting_df <- cznet_datasets_df %>% 
  filter(access == "UNKNOWN") %>%
  rename(apparent_access=access) %>%
  left_join(read_csv(unknown_access_csv),by = c("url","provider","clusters")) %>%
  select(-apparent_access) %>%
  bind_rows(dsp_datasets_df %>%
              filter(access != "UNKNOWN")) %>%
  # add in private datasets from jenkins job
  bind_rows(read_csv(cznet_datasets_jenkins_csv) %>%
              # discoverable == FALSE means the dataset is private 
              filter(discoverable == FALSE) %>%
              # assume submission date is the date it was created
              mutate(dateCreated=as.Date(submission_date)) %>%
              rename(access=discoverable) %>%
              rename(provider=repository) %>%
              select(c('provider','access','dateCreated')) %>%
              filter(dateCreated>=as.Date(date_start)) %>%
              # remove json string elements
              mutate(across(everything(), ~gsub("\\[|'|\\]", "", .))) %>%
              # replace empty strings with na
              mutate(across(everything(), ~na_if(., "")))) %>%
  # remove datasets created 
  filter(dateCreated<=as.Date('2025-05-31'))
  
# generate human readable summary for reporting 
report_summary <- tibble(
  Metric = c(
    "Total data products shared",
    "Number of private datasets",
    "Number of discoverable datasets",
    "Number of public datasets",
    "Number of published datasets",
    "Number of non-private datasets in EarthChem",
    "Number of non-private datasets in HydroShare",
    "Number of non-private datasets in other repositories"
  ),
  Count = c(
    nrow(cznet_datasets_reporting_df %>% filter(access!=FALSE)),
    nrow(filter(cznet_datasets_reporting_df, access == FALSE)),
    nrow(filter(cznet_datasets_reporting_df, access == "DISCOVERABLE")),
    nrow(filter(cznet_datasets_reporting_df, access == "PUBLIC")),
    nrow(filter(cznet_datasets_reporting_df, access == "PUBLISHED")),
    nrow(filter(cznet_datasets_reporting_df %>% filter(access!=FALSE), str_detect(provider, regex("earthchem", ignore_case = TRUE)))),
    nrow(filter(cznet_datasets_reporting_df %>% filter(access!=FALSE), str_detect(provider, regex("hydroshare", ignore_case = TRUE)))),
    nrow(filter(cznet_datasets_reporting_df %>% filter(access!=FALSE), !str_detect(provider, regex("earthchem|hydroshare", ignore_case = TRUE))))
  )
)

kable(report_summary, caption = "Summary of CZNet Datasets")