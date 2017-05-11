from time import time
from datetime import date, datetime

from manager.spark_manager import SparkManager
from iface_extractions.select_fields import *


# /opt/conf/spark-defaults.conf
# #RedshiftJDBC42-1.2.1.1001.jar  http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html#download-jdbc-driver
# #spark-avro_2.11-3.2.0.jar      https://spark-packages.org/package/databricks/spark-avro

# spark.driver.extraClassPath /home/alexis/Downloads/jars/*
# spark.executor.extraClassPath /home/alexis/Downloads/jars/*
# spark.sql.broadcastTimeout 600


if __name__ == "__main__":

    t0 = time()
    print ("Start time: {}".format(datetime.now()))
    # logger.info("Start time: {}".format(datetime.now()))

    spark_manager = SparkManager(URL, DRIVER, USER, PASS)

    # df_fields = direct_fields(spark_manager)
    # print ('TOTAL RECORDS FIRST:', df_fields.count())

    t0 = time()
    print ("Start query: {}".format(datetime.now()))

    # df_fields = select_direct_fields(spark_manager)

    # new select with tables in memomry
    df_fields = direct(spark_manager)

    tt = time() - t0
    print ("End query: {}".format(datetime.now()))
    print ("Elapsed query: {}".format(tt))

    print ('TOTAL RECORDS FIRST:', df_fields.count())

    # get booking's ids lists for seq_rec and seq_reserva

    seq_recs, seq_reservas = get_seq_lists(df_fields)

    #############

    df_fields = sub_tax_sales_transfer_pricing(spark_manager, df_fields, seq_recs, seq_reservas)
    print ('TOTAL RECORDS tax_sales_transfer_pricing:', df_fields.count())

    df_fields = sub_transfer_pricing(spark_manager, df_fields, seq_recs, seq_reservas)
    print ('TOTAL RECORDS transfer_pricing:', df_fields.count())

    df_fields = sub_tax_cost_transfer_pricing(spark_manager, df_fields, seq_recs, seq_reservas)
    print ('TOTAL RECORDS tax_cost_transfer_pricing:', df_fields.count())

    df_fields = sub_tax_sales_transfer_pricing_eur(spark_manager, df_fields, seq_recs, seq_reservas)
    print ('TOTAL RECORDS tax_sales_transfer_pricing_eur:', df_fields.count())

    df_fields = sub_tax_transfer_pricing_eur(spark_manager, df_fields, seq_recs, seq_reservas)
    print ('TOTAL RECORDS tax_transfer_pricing_eur:', df_fields.count())

    df_fields = sub_tax_cost_transfer_pricing_eur(spark_manager, df_fields, seq_recs, seq_reservas)
    print ('TOTAL RECORDS tax_cost_transfer_pricing_eur:', df_fields.count())

    print ('COLUMNS :', df_fields.columns)

    df_fields = remove_fields(df_fields)

    tt = time() - t0
    print("Fields Seconds elapsed: {}".format(tt))
    # logger.info("End time: {}".format(datetime.now()))
    print("Fields End time: {}".format(datetime.now()))

    print("File start time: {}".format(datetime.now()))
    ## CHECK AVRO
    df_fields.write.format("com.databricks.spark.avro").save('daily_iface_extractions_{}'.format(date.today()))

    tt = time() - t0
    print("Seconds elapsed: {}".format(tt))
    # logger.info("End time: {}".format(datetime.now()))
    print("End time: {}".format(datetime.now()))
    # logger.info("Seconds elapsed: {}".format(tt))

    spark_manager.stop_session()

