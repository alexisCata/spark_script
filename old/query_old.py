from functions import *

from iface_extractions.select_fields import *
from manager.spark_manager import SparkManager
from settings import *

# os.environ['SPARK_CLASSPATH'] = "/home/alexis/Downloads/RedshiftJDBC42-1.2.1.1001.jar"  DEPRECATED

# /opt/conf/spark-defaults.conf
# spark.driver.extraClassPath /home/alexis/Downloads/RedshiftJDBC42-1.2.1.1001.jar
# spark.executor.extraClassPath /home/alexis/Downloads/RedshiftJDBC42-1.2.1.1001.jar
# spark.sql.broadcastTimeout 600


if __name__ == "__main__":
    logger.info("HOLA")

    from time import time
    t0 = time()

    spark_manager = SparkManager(URL, DRIVER, USER, PASS)

    df = spark_manager.get_dataframe(tables['acq_atlas_general_gn_t_divisa'])

    df_fields = direct_fields(spark_manager)
    print ('TOTAL RECORDS FIRST:', df_fields.count())

    # get booking's ids lists for seq_rec and seq_reserva
    seq_recs, seq_reservas = get_seq_lists(df_fields)

    # df_fields = sub_first_booking_ts(spark_manager, df_fields, seq_recs, seq_reservas)
    # print ('TOTAL RECORDS first bookin:', df_fields.count())

    # df_fields = sub_destination_code(spark_manager, df_fields, seq_recs, seq_reservas)
    # print ('TOTAL RECORDS destination code:', df_fields.count())

    # df_fields = sub_ttv_booking_currency(spark_manager, df_fields, seq_recs, seq_reservas)
    # print ('TOTAL RECORDS ttv_booking_currency:', df_fields.count())

    # df_fields = sub_tax_ttv(spark_manager, df_fields, seq_recs, seq_reservas)
    # print ('TOTAL RECORDS tax_ttv:', df_fields.count())

    # df_fields = sub_tax_ttv_toms(spark_manager, df_fields, seq_recs, seq_reservas)
    # print ('TOTAL RECORDS tax_ttv_toms:', df_fields.count())

    #############

    # df_fields = sub_tax_sales_transfer_pricing(spark_manager, df_fields, seq_recs, seq_reservas)
    # print ('TOTAL RECORDS tax_sales_transfer_pricing:', df_fields.count())

    # df_fields = sub_transfer_pricing(spark_manager, df_fields, seq_recs, seq_reservas)
    # print ('TOTAL RECORDS transfer_pricing:', df_fields.count())

    # df_fields = sub_tax_cost_transfer_pricing(spark_manager, df_fields, seq_recs, seq_reservas)
    # print ('TOTAL RECORDS tax_cost_transfer_pricing:', df_fields.count())

    # df_fields = sub_tax_sales_transfer_pricing_eur(spark_manager, df_fields, seq_recs, seq_reservas)
    # print ('TOTAL RECORDS tax_sales_transfer_pricing_eur:', df_fields.count())

    # df_fields = sub_tax_transfer_pricing_eur(spark_manager, df_fields, seq_recs, seq_reservas)
    # print ('TOTAL RECORDS tax_transfer_pricing_eur:', df_fields.count())

    df_fields = sub_tax_cost_transfer_pricing_eur(spark_manager, df_fields, seq_recs, seq_reservas)
    print ('TOTAL RECORDS tax_cost_transfer_pricing_eur:', df_fields.count())

    print ('COLUMNS :', df_fields.columns)
    # print ('SHOW :', df_fields.show())
    # print ('TOTAL RECORDS :', df_fields.count())

    tt = time() - t0
    print ('TIME_U: ', tt)

    spark_manager.stop_session()

