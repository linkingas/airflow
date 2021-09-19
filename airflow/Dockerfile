FROM apache/airflow:2.1.3
# this fixes a warning in the official image, with the Azure provider
# More info here https://github.com/apache/airflow/issues/14266#issuecomment-786298240
RUN pip uninstall  --yes azure-storage && pip install -U azure-storage-blob apache-airflow-providers-microsoft-azure==1.1.0
# add your dependencies here:
# RUN pip install package1 package2....