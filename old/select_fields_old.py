import sys

from pyspark.sql import functions as func
from pyspark.sql.types import DecimalType, StringType
from tables import tables

from iface_extractions.functions import *

EUR = 'EUR'
udf_currency_exchange = func.udf(currency_exchange, DecimalType(15, 3))
udf_calculate_night_amount = func.udf(calculate_night_amount, DecimalType(15, 3))
# udf_round_ccy = func.udf(round_ccy, DecimalType())
udf_round_ccy = func.udf(round_ccy, StringType())  # The results have different number of decimals


def direct_fields(manager):
    df_booking = manager.get_dataframe(tables['dwc_bok_t_booking'].format(sys.argv[1], sys.argv[2],
                                                                          sys.argv[1], sys.argv[2]))
    df_ttoo = manager.get_dataframe(tables['dwc_mtd_t_ttoo'])
    df_receptive = manager.get_dataframe(tables['dwc_mtd_t_receptive'])
    df_receptivef = manager.get_dataframe(tables['dwc_mtd_t_receptive_f'])
    df_interface = manager.get_dataframe(tables['dwc_itf_t_fc_interface'])
    df_country = manager.get_dataframe(tables['dwc_gen_t_general_country'])

    df_aux = df_booking.join(df_ttoo, df_booking.gtto_seq_ttoo == df_ttoo.seq_ttoo)

    # CHECK nanvl and try to avoid withColumn
    df_booking_aux = df_aux.withColumn('cod_pais_cliente',
                                       func.when(df_aux.cod_pais_cliente.isNotNull(), df_aux.cod_pais_cliente).
                                       otherwise(df_aux.gpai_cod_pais_mercado))

    df_res = df_booking_aux.join(df_receptive, df_booking_aux.grec_seq_rec == df_receptive.seq_rec_re). \
        join(df_receptivef, df_booking_aux.seq_rec_hbeds == df_receptivef.seq_rec_rf). \
        join(df_interface, df_booking_aux.fint_cod_interface == df_interface.cod_interface). \
        join(df_country, df_booking_aux.cod_pais_cliente == df_country.cod_pais)

    df_res_aux = df_res.selectExpr("CASE WHEN fec_cancelacion is NULL THEN 'N' ELSE 'S' END AS cancelled_booking",
                                   "CASE WHEN partner_ttoo is NULL THEN 'N' ELSE 'S' END AS Partner_booking",
                                   "CASE WHEN ind_tippag is NULL THEN 'Merchant' ELSE 'Pago en hotel' END AS Accomodation_model",
                                   "*")
    df_direct_fields = df_res_aux. \
        select(  # fields for be used with the subselects
        df_res_aux.grec_seq_rec,
        df_res_aux.seq_reserva,
        df_res_aux.semp_cod_emp_re,
        df_res_aux.semp_cod_emp_rf,
        df_res_aux.fec_desde,
        df_res_aux.fec_creacion,
        df_res_aux.gdiv_cod_divisa,
        df_res_aux.ind_tippag,
        df_res_aux.ind_fec_cam_div,
        # result fields
        func.concat(df_res_aux.grec_seq_rec, func.lit('-'), df_res_aux.seq_reserva).alias('interface_id'),
        df_res_aux.semp_cod_emp_re.alias('operative_company'),
        df_res_aux.sofi_cod_ofi_re.alias('operative_office'),
        df_res_aux.des_receptivo.alias('operative_office_desc'),
        df_res_aux.grec_seq_rec.alias('operative_incoming'),
        df_res_aux.seq_reserva.alias('booking_id'),
        df_res_aux.fint_cod_interface.alias('interface'),
        df_res_aux.semp_cod_emp_rf.alias('invoicing_company'),
        df_res_aux.sofi_cod_ofi_rf.alias('invoicing_office'),
        df_res_aux.seq_rec_rf.alias('invoicing_incoming'),
        func.date_format(df_res_aux.fec_creacion, 'yyyy-MM-dd').alias('Creation_date'),
        df_res_aux.fec_creacion.alias('Creation_ts'),
        func.date_format(df_res_aux.fec_modifica, 'yyyy-MM-dd').alias('modification_date'),
        df_res_aux.fec_modifica.alias('modification_ts'),
        func.date_format(df_res_aux.fec_cancelacion, 'yyyy-MM-dd').alias('cancellation_date'),
        df_res_aux.fec_cancelacion.alias('cancellation_ts'),
        # DECODE
        df_res_aux.cancelled_booking,
        func.date_format(func.greatest(df_res_aux.fec_creacion, df_res_aux.fec_modifica, df_res_aux.fec_cancelacion),
                         'yyyy-MM-dd').alias('status_date'),
        df_res_aux.fec_desde.alias('booking_service_from'),
        df_res_aux.fec_hasta.alias('booking_service_to'),
        df_res_aux.seq_ttoo.alias('client_code'),
        df_res_aux.nom_corto_ttoo.alias('costumer_name'),
        df_res_aux.cod_pais_cliente.alias('Source_market'),
        df_res_aux.cod_iso.alias('source_market_iso'),
        func.regexp_replace(df_res_aux.nom_general, ';', '').alias('Holder'),
        df_res_aux.nro_ad.alias('num_adults'),
        df_res_aux.nro_ni.alias('num_childrens'),
        df_res_aux.gdep_cod_depart.alias('Department_code'),
        df_res_aux.rtre_cod_tipo_res.alias('Booking_type'),
        df_res_aux.ind_facturable_res.alias('Invoicing_booking'),
        df_res_aux.ind_facturable_adm.alias('Invoicing_admin'),
        df_res_aux.pct_comision.alias('Client_commision_esp'),
        df_res_aux.pct_rappel.alias('client_override_esp'),
        df_res_aux.ind_confirma.alias('confirmed_booking'),
        # DECODE
        df_res_aux.Partner_booking,
        df_res_aux.cod_divisa_p.alias('Partner_booking_currency'),
        df_res_aux.seq_ttoo_p.alias('Partner_code'),
        df_res_aux.cod_suc_p.alias('Partner_brand'),
        df_res_aux.seq_agencia_p.alias('Partner_agency_code'),
        df_res_aux.seq_sucursal_p.alias('Partner_agency_brand'),
        df_res_aux.seq_rec_expediente.alias('Booking_file_incoming'),
        df_res_aux.seq_res_expediente.alias('booking_file_number'),
        # DECODE
        df_res_aux.Accomodation_model,
        df_res_aux.gdiv_cod_divisa.alias('Booking_currency'))

    return df_direct_fields


def sub_first_booking_ts(manager, df_fields, seq_recs, seq_reservas):
    df_info = manager.get_dataframe(tables['dwc_bok_t_booking_information'])

    df = df_info.groupBy('seq_rec', 'seq_reserva').agg(func.min('fec_creacion_info').alias('First_booking_ts'))

    df_first_booking_ts = df.filter(df.seq_rec.isin(seq_recs)).filter(
        df.seq_reserva.isin(seq_reservas))

    df_fields = df_fields.join(df_first_booking_ts, [df_first_booking_ts.seq_rec == df_fields.grec_seq_rec,
                                                     df_first_booking_ts.seq_reserva == df_fields.seq_reserva]). \
        drop(df_first_booking_ts.seq_rec). \
        drop(df_first_booking_ts.seq_reserva)

    return df_fields


def sub_destination_code(manager, df_fields, seq_recs, seq_reservas):
    """
    Execute the selects to get the field Destination_code
    :param manager:
    :param df_fields:
    :param seq_recs:
    :param seq_reservas:
    :return: dataframe
    """
    df_dest_1 = sub_destination_code_1(manager)
    df_dest_2 = sub_destination_code_2(manager)

    df_destination_code_1 = df_dest_1.filter(df_dest_1.grec_Seq_rec.isin(seq_recs)).filter(
        df_dest_1.rres_seq_reserva.isin(seq_reservas)).dropDuplicates(["grec_Seq_rec", "rres_seq_reserva"])
    df_destination_code_2 = df_dest_2.filter(df_dest_2.seq_rec.isin(seq_recs)).filter(
        df_dest_2.seq_reserva.isin(seq_reservas)).dropDuplicates(["seq_rec", "seq_reserva"])

    df_aux = df_fields.join(df_destination_code_1, [df_fields.grec_seq_rec == df_destination_code_1.grec_Seq_rec,
                                                    df_fields.seq_reserva == df_destination_code_1.rres_seq_reserva],
                            'left_outer').drop(df_destination_code_1.grec_Seq_rec). \
        drop(df_destination_code_1.rres_seq_reserva)

    df_aux2 = df_aux.join(df_destination_code_2, [df_aux.grec_seq_rec == df_destination_code_2.seq_rec,
                                                  df_aux.seq_reserva == df_destination_code_2.seq_reserva],
                          'left_outer').drop(df_destination_code_2.seq_rec). \
        drop(df_destination_code_2.seq_reserva)

    df_destination_code = df_aux2.withColumn('Destination_code',
                                             func.when(df_aux2.izge_cod_destino.isNotNull(),
                                                       func.concat(df_aux2.izge_cod_destino, func.lit('-'),
                                                                   df_aux2.nom_destino)).
                                             otherwise(
                                                 func.concat(df_aux2.cod_destino, func.lit('-'), df_aux2.nom_destino2)))

    df_res = df_destination_code.drop(df_destination_code.izge_cod_destino).drop(df_destination_code.nom_destino).drop(
        df_destination_code.cod_destino).drop(
        df_destination_code.nom_destino2)

    return df_res


def sub_destination_code_1(manager):
    df_1 = manager.get_dataframe(tables['dwc_mtd_t_hotel'])
    df_2 = manager.get_dataframe(tables['dwc_bok_t_hotel_sale'])
    df_3 = manager.get_dataframe(tables['dwc_itn_t_internet_destination_id'])
    df_3 = df_3.filter(df_3.sidi_cod_idioma == 'ENG')

    df_res = df_1.join(df_2, df_1.seq_hotel == df_2.ghor_seq_hotel). \
        join(df_3, df_1.izge_cod_destino == df_3.ides_cod_destino). \
        select("grec_Seq_rec", "rres_seq_reserva", "izge_cod_destino", "nom_destino")

    return df_res.dropDuplicates()


def sub_destination_code_2(manager):
    df_1 = manager.get_dataframe(tables['dwc_bok_t_other'])
    df_2 = manager.get_dataframe(tables['dwc_con_t_contract_other'])
    df_3 = manager.get_dataframe(tables['dwc_itn_t_internet_destination_id'])
    df_3 = df_3.filter(df_3.sidi_cod_idioma == 'ENG')

    df_res = df_1.join(df_2, ["seq_rec", "nom_contrato", "ind_tipo_otro"]). \
        filter(((df_1.fec_desde >= df_2.fec_desde) & (df_1.fec_desde <= df_2.fec_hasta))). \
        join(df_3, df_2.cod_destino == df_3.ides_cod_destino). \
        select("seq_rec", "seq_reserva", "cod_destino", df_3.nom_destino.alias("nom_destino2"))

    return df_res.dropDuplicates()


def get_seq_lists(dataframe_bookings):
    """
    Returns two lists with the ids of grec_seq_rec and seq_reserva
    :param dataframe_bookings: 
    :return: list, list
    """
    seq_rec = dataframe_bookings.select('grec_seq_rec').collect()
    seq_reserva = dataframe_bookings.select('seq_reserva').collect()
    seq_rec = [val[0] for val in list(seq_rec)]
    seq_reserva = [val[0] for val in list(seq_reserva)]

    return seq_rec, seq_reserva


def sub_ttv_booking_currency(manager, df_fields, seq_recs, seq_reservas):
    types = ['O', 'V', 'D', 'W', 'Y', 'IT']

    df_sale = manager.get_dataframe(tables['dwc_bok_t_sale'])

    df_sale = df_sale.drop("ind_trfprc", "imp_impuesto", "ind_tipo_regimen", "rvp_ind_tipo_imp", "rvp_cod_impuesto",
                           "rvp_cod_esquema", "rvp_cod_clasif", "rvp_cod_empresa")

    df_sale = df_sale.filter(df_sale.grec_seq_rec_s.isin(seq_recs)) \
        .filter(df_sale.rres_seq_reserva.isin(seq_reservas)) \
        .filter(df_sale.ind_tipo_registro.isin(types) == False) \
        .filter(df_sale.ind_contra_apunte == 'N')

    # print('COUNT 1: ', df_sale.count())

    df_aux = df_fields.join(df_sale, [df_sale.grec_seq_rec_s == df_fields.grec_seq_rec,
                                      df_sale.rres_seq_reserva == df_fields.seq_reserva], 'left_outer') \
        .drop(df_fields.grec_seq_rec).drop(df_fields.seq_reserva) \
        .filter((df_fields.ind_tippag.isNull() & (df_sale.ind_facturable == 'S')) |
                (df_fields.ind_tippag.isNotNull() & (df_sale.ind_tipo_registro != 'CH')))

    # print('COUNT 2: ', df_aux.count())

    df_fields = df_fields.withColumn("date_ccy_exchange",
                                     func.when(df_fields.ind_fec_cam_div == 'E', df_fields.fec_desde)
                                     .otherwise(df_fields.fec_creacion))

    df_aux = df_aux.withColumn('night_amount',
                               udf_calculate_night_amount(df_aux.fec_desde_s,
                                                          df_aux.fec_hasta_s,
                                                          df_aux.nro_unidades,
                                                          df_aux.nro_pax,
                                                          df_aux.ind_tipo_unid,
                                                          df_aux.ind_p_s,
                                                          df_aux.imp_unitario))

    df_aux = df_aux.select('grec_seq_rec_s', 'rres_seq_reserva', 'night_amount')

    df_fields = df_fields.join(df_aux, [df_fields.grec_seq_rec == df_aux.grec_seq_rec_s,
                                        df_fields.seq_reserva == df_aux.rres_seq_reserva], 'left_outer') \
        .drop(df_aux.grec_seq_rec_s).drop(df_aux.rres_seq_reserva)

    # print('COUNT 3: ', df_fields.count())

    df_fields = df_fields.withColumn('night_amount',
                                     func.when(df_fields.night_amount.isNull(), 0)
                                     .otherwise(df_fields.night_amount))

    df_fields = df_fields.withColumn('ccy_exchange',
                                     udf_currency_exchange(df_fields.semp_cod_emp_rf,
                                                           func.lit(EXCHANGE_RES),
                                                           df_fields.date_ccy_exchange,
                                                           df_fields.gdiv_cod_divisa,
                                                           df_fields.night_amount,
                                                           df_fields.gdiv_cod_divisa))

    df_fields = df_fields.drop(df_fields.date_ccy_exchange).drop(df_fields.night_amount)

    # print('COUNT 4: ', df_fields.count())

    df_fields = df_fields.groupBy('grec_seq_rec', 'seq_reserva', 'semp_cod_emp_re', 'semp_cod_emp_rf', 'fec_desde',
                                  'fec_creacion', 'gdiv_cod_divisa', 'ind_tippag', 'ind_fec_cam_div', 'interface_id',
                                  'operative_company', 'operative_office', 'operative_office_desc',
                                  'operative_incoming', 'booking_id', 'interface', 'invoicing_company',
                                  'invoicing_office', 'invoicing_incoming', 'Creation_date', 'Creation_ts',
                                  'modification_date', 'modification_ts', 'cancellation_date', 'cancellation_ts',
                                  'cancelled_booking', 'status_date', 'booking_service_from', 'booking_service_to',
                                  'client_code', 'costumer_name', 'Source_market', 'source_market_iso', 'Holder',
                                  'num_adults', 'num_childrens', 'Department_code', 'Booking_type', 'Invoicing_booking',
                                  'Invoicing_admin', 'Client_commision_esp', 'client_override_esp', 'confirmed_booking',
                                  'Partner_booking', 'Partner_booking_currency', 'Partner_code', 'Partner_brand',
                                  'Partner_agency_code', 'Partner_agency_brand', 'Booking_file_incoming',
                                  # First_booking_ts, Destination_code,
                                  'booking_file_number', 'Accomodation_model', 'Booking_currency') \
        .agg({'ccy_exchange': 'sum'}).withColumnRenamed("SUM(ccy_exchange)", "TTV_booking_currency")

    # print('COUNT 5: ', df_fields.count())

    return df_fields


def sub_tax(manager, df_fields, seq_recs, seq_reservas, type):
    types = ['O', 'D', 'IT']

    df_sale = manager.get_dataframe(tables['dwc_bok_t_sale'])

    df_imp = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap'])

    df_sale = df_sale.filter(df_sale.grec_seq_rec_s.isin(seq_recs)) \
        .filter(df_sale.rres_seq_reserva.isin(seq_reservas)) \
        .filter(df_sale.ind_tipo_registro.isin(types) == False) \
        .filter(df_sale.ind_contra_apunte == 'N')

    df_aux = df_fields.join(df_sale, [df_sale.grec_seq_rec_s == df_fields.grec_seq_rec,
                                      df_sale.rres_seq_reserva == df_fields.seq_reserva], 'left_outer') \
        .drop(df_fields.grec_seq_rec).drop(df_fields.seq_reserva) \
        .filter((df_fields.ind_tippag.isNull() & (df_sale.ind_facturable == 'S')) |
                (df_fields.ind_tippag.isNotNull() & (df_sale.ind_tipo_registro != 'CH')))

    df_tax = df_aux.join(df_imp, [df_aux.rvp_ind_tipo_imp == df_imp.ind_tipo_imp,
                                  df_aux.rvp_cod_impuesto == df_imp.cod_impuesto,
                                  df_aux.rvp_cod_esquema == df_imp.cod_esquema,
                                  df_aux.rvp_cod_clasif == df_imp.cod_clasif,
                                  df_aux.rvp_cod_empresa == df_imp.cod_emp_atlas], 'left_outer')

    df_tax = df_tax.withColumn("date_ccy_exchange",
                               func.when(df_tax.ind_fec_cam_div == 'E', df_tax.fec_desde)
                               .otherwise(df_tax.fec_creacion))

    df_tax = df_tax.withColumn('night_amount',
                               udf_calculate_night_amount(df_tax.fec_desde_s,
                                                          df_tax.fec_hasta_s,
                                                          df_tax.nro_unidades,
                                                          df_tax.nro_pax,
                                                          df_tax.ind_tipo_unid,
                                                          df_tax.ind_p_s,
                                                          (df_tax.imp_unitario - (df_tax.imp_unitario / (
                                                              1 + (df_tax.pct_impuesto / 100))))))

    # REVISAR
    df_tax = df_tax.withColumn('tax_aux3',
                               func.when(df_tax.imp_impuesto.isNull(), df_tax.night_amount)
                               .otherwise(df_tax.imp_impuesto))

    df_tax = df_tax.withColumn('tax_aux2',
                               func.when(func.concat(df_tax.ind_tipo_regimen,
                                                     func.when(df_tax.ind_trfprc.isNull(), 'N')
                                                     .otherwise(df_tax.ind_trfprc)) == type,
                                         df_tax.tax_aux3)
                               .otherwise(0))  # esto me lo he inventado :D

    df_tax = df_tax.withColumn('tax_aux',
                               func.when(df_tax.ind_tipo_registro == 'BT', 0)
                               .otherwise(df_tax.tax_aux2))

    df_tax = df_tax.select('grec_seq_rec_s', 'rres_seq_reserva', 'date_ccy_exchange', 'tax_aux')

    df_fields = df_fields.join(df_tax, [df_fields.grec_seq_rec == df_tax.grec_seq_rec_s,
                                        df_fields.seq_reserva == df_tax.rres_seq_reserva], 'left_outer') \
        .drop(df_tax.grec_seq_rec_s).drop(df_tax.rres_seq_reserva)

    df_fields = df_fields.withColumn('ccy_exchange',
                                     udf_currency_exchange(df_fields.semp_cod_emp_rf,
                                                           func.lit(EXCHANGE_RES),
                                                           df_fields.date_ccy_exchange,
                                                           df_fields.gdiv_cod_divisa,
                                                           df_fields.tax_aux,
                                                           df_fields.gdiv_cod_divisa))

    df_fields = df_fields.drop(df_fields.date_ccy_exchange).drop(df_fields.tax_aux)

    return df_fields


def sub_tax_ttv(manager, df_fields, seq_recs, seq_reservas):
    df_fields = sub_tax(manager, df_fields, seq_recs, seq_reservas, 'GN')

    df_fields = df_fields.groupBy('grec_seq_rec', 'seq_reserva', 'semp_cod_emp_re', 'semp_cod_emp_rf', 'fec_desde',
                                  'fec_creacion', 'gdiv_cod_divisa', 'ind_tippag', 'ind_fec_cam_div', 'interface_id',
                                  'operative_company', 'operative_office', 'operative_office_desc',
                                  'operative_incoming', 'booking_id', 'interface', 'invoicing_company',
                                  'invoicing_office', 'invoicing_incoming', 'Creation_date', 'Creation_ts',
                                  'modification_date', 'modification_ts', 'cancellation_date', 'cancellation_ts',
                                  'cancelled_booking', 'status_date', 'booking_service_from', 'booking_service_to',
                                  'client_code', 'costumer_name', 'Source_market', 'source_market_iso', 'Holder',
                                  'num_adults', 'num_childrens', 'Department_code', 'Booking_type', 'Invoicing_booking',
                                  'Invoicing_admin', 'Client_commision_esp', 'client_override_esp', 'confirmed_booking',
                                  'Partner_booking', 'Partner_booking_currency', 'Partner_code', 'Partner_brand',
                                  'Partner_agency_code', 'Partner_agency_brand', 'Booking_file_incoming',
                                  'booking_file_number', 'Accomodation_model', 'Booking_currency'
                                  # First_booking_ts, Destination_code, TTV_booking_currency
                                  ) \
        .agg({'ccy_exchange': 'sum'}).withColumnRenamed("SUM(ccy_exchange)", "Tax_TTV")

    df_fields = df_fields.na.fill({'Tax_TTV': 0})

    return df_fields


def sub_tax_ttv_toms(manager, df_fields, seq_recs, seq_reservas):
    df_fields = sub_tax(manager, df_fields, seq_recs, seq_reservas, 'EN')

    df_fields = df_fields.groupBy('grec_seq_rec', 'seq_reserva', 'semp_cod_emp_re', 'semp_cod_emp_rf', 'fec_desde',
                                  'fec_creacion', 'gdiv_cod_divisa', 'ind_tippag', 'ind_fec_cam_div', 'interface_id',
                                  'operative_company', 'operative_office', 'operative_office_desc',
                                  'operative_incoming', 'booking_id', 'interface', 'invoicing_company',
                                  'invoicing_office', 'invoicing_incoming', 'Creation_date', 'Creation_ts',
                                  'modification_date', 'modification_ts', 'cancellation_date', 'cancellation_ts',
                                  'cancelled_booking', 'status_date', 'booking_service_from', 'booking_service_to',
                                  'client_code', 'costumer_name', 'Source_market', 'source_market_iso', 'Holder',
                                  'num_adults', 'num_childrens', 'Department_code', 'Booking_type', 'Invoicing_booking',
                                  'Invoicing_admin', 'Client_commision_esp', 'client_override_esp', 'confirmed_booking',
                                  'Partner_booking', 'Partner_booking_currency', 'Partner_code', 'Partner_brand',
                                  'Partner_agency_code', 'Partner_agency_brand', 'Booking_file_incoming',
                                  'booking_file_number', 'Accomodation_model', 'Booking_currency'
                                  # First_booking_ts, Destination_code, TTV_booking_currency, Tax_TTV
                                  ) \
        .agg({'ccy_exchange': 'sum'}).withColumnRenamed("SUM(ccy_exchange)", "Tax_TTV_TOMS")

    df_fields = df_fields.na.fill({'Tax_TTV_TOMS': 0})

    return df_fields


def sub_tax_sales_transfer_pricing_aux(manager, dataframe, seq_recs, seq_reservas):
    df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
        .filter(dataframe.seq_reserva.isin(seq_reservas))

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_con", "imp_margen_canco",
                "imp_venta", "pct_impuesto_vta", "pct_impuesto_cpa")

    dataframe = dataframe.withColumn("impuesto_canal",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                                            / 100)))))
                                     .otherwise((dataframe.imp_venta + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_venta) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100)))))) \
        .select("seq_rec", "seq_reserva", "impuesto_canal")

    dataframe = dataframe.groupBy("seq_rec", "seq_reserva") \
        .agg({'impuesto_canal': 'sum'}).withColumnRenamed("SUM(impuesto_canal)", "impuesto_canal")

    return dataframe


def sub_tax_sales_transfer_pricing_aux_extra(manager, dataframe, seq_recs, seq_reservas):
    df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
        .filter(dataframe.seq_reserva.isin(seq_reservas))

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_con", "imp_margen_canco",
                "imp_venta", "pct_impuesto_vta", "pct_impuesto_cpa", "ord_extra")

    dataframe = dataframe.withColumn("impuesto_canal",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                                            / 100)))))
                                     .otherwise((dataframe.imp_venta + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_venta) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    # FILTER WHERE IN
    df_bok = manager.get_dataframe(tables['dwc_bok_t_extra'])
    df_discount = manager.get_dataframe(tables['dwc_cli_dir_t_cd_discount_bond'])
    df_campaign = manager.get_dataframe(tables['dwc_cli_dir_t_cd_campaign'])

    df_campaign = df_campaign.filter(df_campaign.ind_rentabilidad == 'N')

    df_filter = df_bok.join(df_discount, [df_bok.num_bono == df_discount.num_bono,
                                          df_bok.cod_interface == df_discount.cod_interface]) \
        .join(df_campaign, [df_discount.cod_campana == df_campaign.cod_campana,
                            df_discount.cod_interface == df_campaign.cod_interface]).select("grec_seq_rec",
                                                                                            "rres_seq_reserva",
                                                                                            "ord_extra")

    df_aux_filter = dataframe.join(df_filter, (dataframe.seq_rec == df_filter.grec_seq_rec) &
                                   (dataframe.seq_reserva == df_filter.rres_seq_reserva) &
                                   (dataframe.ord_extra == df_filter.ord_extra), 'left_outer') \
        .select(df_filter.grec_seq_rec, df_filter.rres_seq_reserva, df_filter.ord_extra) \
        .filter((df_filter.grec_seq_rec.isNull() == False) &
                (df_filter.rres_seq_reserva.isNull() == False) &
                (df_filter.ord_extra.isNull() == False))

    dataframe = dataframe.join(df_aux_filter, (dataframe.seq_rec != df_aux_filter.grec_seq_rec) &
                               (dataframe.seq_reserva != df_aux_filter.rres_seq_reserva) &
                               (dataframe.ord_extra != df_aux_filter.ord_extra),
                               'left_outer') \
        .drop(df_aux_filter.grec_seq_rec).drop(df_aux_filter.rres_seq_reserva).drop(df_aux_filter.ord_extra)

    dataframe = dataframe.withColumnRenamed("seq_rec", "grec_seq_rec")

    dataframe = dataframe.groupBy("grec_seq_rec", "seq_reserva").agg({'impuesto_canal': 'sum'}) \
        .withColumnRenamed("SUM(impuesto_canal)", "impuesto_canal")

    return dataframe


def sub_tax_sales_transfer_pricing(manager, df_fields, seq_recs, seq_reservas):
    df_hotel = manager.get_dataframe(tables['dwc_bok_t_canco_hotel'])
    df_circuit = manager.get_dataframe(tables['dwc_bok_t_canco_hotel_circuit'])
    df_other = manager.get_dataframe(tables['dwc_bok_t_canco_other'])
    df_transfer = manager.get_dataframe(tables['dwc_bok_t_canco_transfer'])
    df_endow = manager.get_dataframe(tables['dwc_bok_t_canco_endowments'])
    df_extra = manager.get_dataframe(tables['dwc_bok_t_canco_extra'])

    df_hotel = sub_tax_sales_transfer_pricing_aux(manager, df_hotel, seq_recs, seq_reservas)
    df_circuit = sub_tax_sales_transfer_pricing_aux(manager, df_circuit, seq_recs, seq_reservas)
    df_other = sub_tax_sales_transfer_pricing_aux(manager, df_other, seq_recs, seq_reservas)
    df_transfer = sub_tax_sales_transfer_pricing_aux(manager, df_transfer, seq_recs, seq_reservas)
    df_endow = sub_tax_sales_transfer_pricing_aux(manager, df_endow, seq_recs, seq_reservas)
    df_extra = sub_tax_sales_transfer_pricing_aux_extra(manager, df_extra, seq_recs, seq_reservas)

    df_impuesto_canal = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(df_extra)

    df_impuesto_canal = df_impuesto_canal.groupBy("seq_rec", "seq_reserva") \
        .agg({'impuesto_canal': 'sum'}).withColumnRenamed("SUM(impuesto_canal)", "Tax_Sales_Transfer_pricing")

    df_fields = df_fields.join(df_impuesto_canal, [df_fields.grec_seq_rec == df_impuesto_canal.seq_rec,
                                                   df_fields.seq_reserva == df_impuesto_canal.seq_reserva],
                               'left_outer').drop(df_impuesto_canal.seq_rec).drop(df_impuesto_canal.seq_reserva)

    df_fields = df_fields.na.fill({"Tax_Sales_Transfer_pricing": 0})

    df_fields = df_fields.withColumn("Tax_Sales_Transfer_pricing", udf_round_ccy(df_fields.Tax_Sales_Transfer_pricing,
                                                                                 df_fields.gdiv_cod_divisa))

    return df_fields


def sub_transfer_pricing_aux(manager, dataframe, seq_recs, seq_reservas, df_aux):
    df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
        .filter(dataframe.seq_reserva.isin(seq_reservas))

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_con", "ind_tipo_regimen_fac", "imp_margen_canco",
                "imp_venta", "imp_coste", "pct_impuesto_vta", "pct_impuesto_cpa", "imp_margen_canal")

    dataframe = dataframe.withColumn("impuesto_canco1",
                                     func.when(dataframe.ind_tipo_regimen_fac == 'E',
                                               dataframe.imp_margen_canal * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))))
                                     .otherwise(
                                         dataframe.imp_venta * (1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100)))) - (
                                             dataframe.imp_venta - dataframe.imp_margen_canal) * (
                                             1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))
                                     ))

    dataframe = dataframe.withColumn("impuesto_canco2",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                                            / 100)))))
                                     .otherwise((dataframe.imp_coste + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_coste) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.grec_seq_rec,
                                        dataframe.seq_reserva == df_aux.seq_reserva]).drop(df_aux.seq_reserva)

    dataframe = dataframe.na.fill({'impuesto_canco1': 0, 'impuesto_canco2': 0})

    dataframe = dataframe.withColumn('impuesto_canco2', udf_currency_exchange(dataframe.semp_cod_emp_rf,
                                                                              func.lit(EXCHANGE_RES),
                                                                              dataframe.fec_creacion,
                                                                              dataframe.gdiv_cod_divisa,
                                                                              dataframe.impuesto_canco2,
                                                                              func.lit(EUR)))

    dataframe = dataframe.select("grec_seq_rec", "seq_reserva", "impuesto_canco1", "impuesto_canco2")

    dataframe = dataframe.groupBy("grec_seq_rec", "seq_reserva").agg({'impuesto_canco1': 'sum',
                                                                      'impuesto_canco2': 'sum'}) \
        .withColumnRenamed("SUM(impuesto_canco1)", "impuesto_canco1") \
        .withColumnRenamed("SUM(impuesto_canco2)", "impuesto_canco2")

    dataframe = dataframe.withColumn("impuesto_canco",
                                     dataframe.impuesto_canco1 + dataframe.impuesto_canco2) \
        .drop("impuesto_canco1", "impuesto_canco2").na.fill({"impuesto_canco": 0})

    return dataframe


def sub_transfer_pricing_aux_extra(manager, dataframe, seq_recs, seq_reservas, df_aux):
    df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
        .filter(dataframe.seq_reserva.isin(seq_reservas))

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_con", "ind_tipo_regimen_fac", "imp_margen_canco",
                "imp_venta", "imp_coste", "pct_impuesto_vta", "pct_impuesto_cpa", "imp_margen_canal", "ord_extra")

    dataframe = dataframe.withColumn("impuesto_canco2",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                                            / 100)))))
                                     .otherwise((dataframe.imp_coste + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_coste) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.withColumn("impuesto_canco1",
                                     func.when(dataframe.ind_tipo_regimen_fac == 'E',
                                               dataframe.imp_margen_canal * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))))
                                     .otherwise(
                                         dataframe.imp_venta * (1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100)))) - (
                                             dataframe.imp_venta - dataframe.imp_margen_canal) * (
                                             1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))
                                     ))

    dataframe = dataframe.na.fill({'impuesto_canco1': 0, 'impuesto_canco2': 0})

    # FILTER WHERE IN
    df_bok = manager.get_dataframe(tables['dwc_bok_t_extra'])
    df_discount = manager.get_dataframe(tables['dwc_cli_dir_t_cd_discount_bond'])
    df_campaign = manager.get_dataframe(tables['dwc_cli_dir_t_cd_campaign'])

    df_campaign = df_campaign.filter(df_campaign.ind_rentabilidad == 'N')

    df_filter = df_bok.join(df_discount, [df_bok.num_bono == df_discount.num_bono,
                                          df_bok.cod_interface == df_discount.cod_interface]) \
        .join(df_campaign, [df_discount.cod_campana == df_campaign.cod_campana,
                            df_discount.cod_interface == df_campaign.cod_interface]).select("grec_seq_rec",
                                                                                            "rres_seq_reserva",
                                                                                            "ord_extra")

    df_aux_filter = dataframe.join(df_filter, (dataframe.seq_rec == df_filter.grec_seq_rec) &
                                   (dataframe.seq_reserva == df_filter.rres_seq_reserva) &
                                   (dataframe.ord_extra == df_filter.ord_extra), 'left_outer') \
        .select(df_filter.grec_seq_rec, df_filter.rres_seq_reserva, df_filter.ord_extra) \
        .filter((df_filter.grec_seq_rec.isNull() == False) &
                (df_filter.rres_seq_reserva.isNull() == False) &
                (df_filter.ord_extra.isNull() == False))

    dataframe = dataframe.join(df_aux_filter, (dataframe.seq_rec != df_aux_filter.grec_seq_rec) &
                               (dataframe.seq_reserva != df_aux_filter.rres_seq_reserva) &
                               (dataframe.ord_extra != df_aux_filter.ord_extra),
                               'left_outer') \
        .drop(df_aux_filter.grec_seq_rec).drop(df_aux_filter.rres_seq_reserva).drop(df_aux_filter.ord_extra)

    dataframe = dataframe.na.fill({'impuesto_canco1': 0, 'impuesto_canco2': 0})

    # join with the main ids "df_fields"
    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.grec_seq_rec,
                                        dataframe.seq_reserva == df_aux.seq_reserva]).drop(df_aux.seq_reserva)

    dataframe = dataframe.withColumn('impuesto_canco2', udf_currency_exchange(dataframe.semp_cod_emp_rf,
                                                                              func.lit(EXCHANGE_RES),
                                                                              dataframe.fec_creacion,
                                                                              dataframe.gdiv_cod_divisa,
                                                                              dataframe.impuesto_canco2,
                                                                              func.lit(EUR)))

    dataframe = dataframe.select("grec_seq_rec", "seq_reserva", "impuesto_canco1", "impuesto_canco2")

    dataframe = dataframe.groupBy("grec_seq_rec", "seq_reserva").agg({'impuesto_canco1': 'sum',
                                                                      'impuesto_canco2': 'sum'}) \
        .withColumnRenamed("SUM(impuesto_canco1)", "impuesto_canco1") \
        .withColumnRenamed("SUM(impuesto_canco2)", "impuesto_canco2")

    dataframe = dataframe.withColumn("impuesto_canco",
                                     dataframe.impuesto_canco1 + dataframe.impuesto_canco2) \
        .drop("impuesto_canco1", "impuesto_canco2").na.fill({"impuesto_canco": 0})

    return dataframe


def sub_transfer_pricing_aux_add_canco(manager, df_fields, seq_recs, seq_reservas, eur=None):
    df_ids = df_fields.select("grec_seq_rec", "seq_reserva", "semp_cod_emp_rf", "fec_creacion",
                              "gdiv_cod_divisa")  # ids main select fields

    df_cost = manager.get_dataframe(tables["dwc_bok_t_cost"])
    df_cost = df_cost.filter(df_cost.ind_tipo_registro == 'DR').filter(df_cost.ind_facturable == 'S') \
        .filter(df_cost.grec_seq_rec.isin(seq_recs)).filter(df_cost.rres_seq_reserva.isin(seq_reservas)) \
        .select("grec_seq_rec", "rres_seq_reserva", "rext_ord_extra", "sdiv_cod_divisa", "fec_desde", "fec_hasta",
                "nro_unidades", "nro_pax", "ind_tipo_unid", "ind_p_s", "imp_unitario")

    # FILTER NOT EXISTS
    df_bok = manager.get_dataframe(tables['dwc_bok_t_extra'])
    df_discount = manager.get_dataframe(tables['dwc_cli_dir_t_cd_discount_bond'])
    df_campaign = manager.get_dataframe(tables['dwc_cli_dir_t_cd_campaign'])

    df_campaign = df_campaign.filter(df_campaign.ind_rentabilidad == 'N')

    df_filter = df_bok.join(df_discount, [df_bok.num_bono == df_discount.num_bono,
                                          df_bok.cod_interface == df_discount.cod_interface]) \
        .join(df_campaign, [df_discount.cod_campana == df_campaign.cod_campana,
                            df_discount.cod_interface == df_campaign.cod_interface]).select("grec_seq_rec",
                                                                                            "rres_seq_reserva",
                                                                                            "ord_extra")
    df_filter = df_filter.filter(df_filter.grec_seq_rec.isin(seq_recs)).filter(
        df_filter.rres_seq_reserva.isin(seq_reservas))

    # need ids to exclude

    df_aux_filter = df_cost.join(df_filter, (df_cost.grec_seq_rec == df_filter.grec_seq_rec) &
                                 (df_cost.rres_seq_reserva == df_filter.rres_seq_reserva) &
                                 (df_cost.rext_ord_extra == df_filter.ord_extra), 'left_outer') \
        .filter((df_filter.grec_seq_rec.isNull() == False) &
                (df_filter.rres_seq_reserva.isNull() == False) &
                (df_filter.ord_extra.isNull() == False)) \
        .select(df_filter.grec_seq_rec, df_filter.rres_seq_reserva, df_filter.ord_extra)

    df_res = df_cost.join(df_aux_filter, (df_cost.grec_seq_rec != df_aux_filter.grec_seq_rec) &
                          (df_cost.rres_seq_reserva != df_aux_filter.rres_seq_reserva) &
                          (df_cost.rext_ord_extra != df_aux_filter.ord_extra),
                          'left_outer') \
        .drop(df_aux_filter.grec_seq_rec).drop(df_aux_filter.rres_seq_reserva).drop(df_aux_filter.ord_extra)

    df_res = df_res.join(df_ids, [df_res.grec_seq_rec == df_ids.grec_seq_rec,
                                  df_res.rres_seq_reserva == df_ids.seq_reserva]) \
        .select(df_ids.grec_seq_rec, df_ids.seq_reserva, df_ids.semp_cod_emp_rf, df_ids.fec_creacion,
                df_ids.gdiv_cod_divisa,
                df_res.rext_ord_extra, df_res.sdiv_cod_divisa, df_res.fec_desde, df_res.fec_hasta, df_res.nro_unidades,
                df_res.nro_pax, df_res.ind_tipo_unid, df_res.ind_p_s, df_res.imp_unitario)

    df_res = df_res.withColumn("night_amount", udf_calculate_night_amount(df_res.fec_desde,
                                                                          df_res.fec_hasta,
                                                                          df_res.nro_unidades,
                                                                          df_res.nro_pax,
                                                                          df_res.ind_tipo_unid,
                                                                          df_res.ind_p_s,
                                                                          df_res.imp_unitario))

    if not eur:
        df_res = df_res.withColumn('add_impuesto_canco',
                                   udf_currency_exchange(df_res.semp_cod_emp_rf,
                                                         func.lit(EXCHANGE_RES),
                                                         df_res.fec_creacion,
                                                         df_res.sdiv_cod_divisa,
                                                         df_res.night_amount,
                                                         df_res.gdiv_cod_divisa))
    else:
        df_res = df_res.withColumn('add_impuesto_canco',
                                   udf_currency_exchange(df_res.semp_cod_emp_rf,
                                                         func.lit(EXCHANGE_RES),
                                                         df_res.fec_creacion,
                                                         df_res.sdiv_cod_divisa,
                                                         df_res.night_amount,
                                                         func.lit(EUR)))

    df_res = df_res.na.fill({'add_impuesto_canco': 0})

    df_addcanco = df_res.groupBy("grec_seq_rec", "seq_reserva") \
        .agg({'add_impuesto_canco': 'sum'}).withColumnRenamed("SUM(add_impuesto_canco)", "add_impuesto_canco")

    return df_addcanco


def sub_transfer_pricing(manager, df_fields, seq_recs, seq_reservas):
    df_hotel = manager.get_dataframe(tables["dwc_bok_t_canco_hotel"])
    df_circuit = manager.get_dataframe(tables["dwc_bok_t_canco_hotel_circuit"])
    df_other = manager.get_dataframe(tables["dwc_bok_t_canco_other"])
    df_transfer = manager.get_dataframe(tables["dwc_bok_t_canco_transfer"])
    df_endow = manager.get_dataframe(tables["dwc_bok_t_canco_endowments"])
    df_extra = manager.get_dataframe(tables["dwc_bok_t_canco_extra"])

    df_aux = df_fields.select("grec_seq_rec", "seq_reserva", "semp_cod_emp_rf", "fec_creacion", "gdiv_cod_divisa")

    df_hotel = sub_transfer_pricing_aux(manager, df_hotel, seq_recs, seq_reservas, df_aux)
    df_circuit = sub_transfer_pricing_aux(manager, df_circuit, seq_recs, seq_reservas, df_aux)
    df_other = sub_transfer_pricing_aux(manager, df_other, seq_recs, seq_reservas, df_aux)
    df_transfer = sub_transfer_pricing_aux(manager, df_transfer, seq_recs, seq_reservas, df_aux)
    df_endow = sub_transfer_pricing_aux(manager, df_endow, seq_recs, seq_reservas, df_aux)
    df_extra = sub_transfer_pricing_aux_extra(manager, df_extra, seq_recs, seq_reservas, df_aux)

    df_impuesto_canco = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(df_extra)

    #

    df_impuesto_canco = df_impuesto_canco.groupBy("grec_seq_rec", "seq_reserva") \
        .agg({'impuesto_canco': 'sum'}).withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    df_impuesto_canco = df_impuesto_canco.withColumnRenamed("grec_seq_rec", "seq_rec") \
        .withColumnRenamed("seq_reserva", "seq_res")

    # add impuesto_canco
    df_fields = df_fields.join(df_impuesto_canco, [df_fields.grec_seq_rec == df_impuesto_canco.seq_rec,
                                                   df_fields.seq_reserva == df_impuesto_canco.seq_res],
                               'left_outer').drop("seq_rec", "seq_res")

    df_addcanco = sub_transfer_pricing_aux_add_canco(manager, df_fields, seq_recs, seq_reservas)

    df_addcanco = df_addcanco.withColumnRenamed("grec_seq_rec", "seq_rec") \
        .withColumnRenamed("seq_reserva", "seq_res")

    # add add_impuesto_canco
    df_fields = df_fields.join(df_addcanco, [df_fields.grec_seq_rec == df_addcanco.seq_rec,
                                             df_fields.seq_reserva == df_addcanco.seq_res],
                               "left_outer").drop(df_addcanco.seq_rec).drop(df_addcanco.seq_res)

    df_fields = df_fields.na.fill({'impuesto_canco': 0, 'add_impuesto_canco': 0})

    df_fields = df_fields.withColumn("Transfer_pricing", df_fields.impuesto_canco + df_fields.add_impuesto_canco) \
        .drop("impuesto_canco", "add_impuesto_canco")

    df_fields = df_fields.withColumn("Transfer_pricing", udf_round_ccy(df_fields.Transfer_pricing,
                                                                       df_fields.gdiv_cod_divisa))

    return df_fields


def sub_tax_cost_transfer_pricing_aux(manager, dataframe, seq_recs, seq_reservas, df_aux):
    df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
        .filter(dataframe.seq_reserva.isin(seq_reservas))

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_con", "imp_margen_canco",
                "imp_coste", "pct_impuesto_vta", "pct_impuesto_cpa")

    dataframe = dataframe.withColumn("impuesto_canco",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                                            / 100)))))
                                     .otherwise((dataframe.imp_coste + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_coste) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.grec_seq_rec,
                                        dataframe.seq_reserva == df_aux.seq_reserva]).drop(df_aux.seq_reserva)

    dataframe = dataframe.na.fill({'impuesto_canco': 0})

    dataframe = dataframe.select("grec_seq_rec", "seq_reserva", "impuesto_canco")

    dataframe = dataframe.groupBy("grec_seq_rec", "seq_reserva").agg({'impuesto_canco': 'sum'}) \
        .withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    return dataframe


def sub_tax_cost_transfer_pricing_aux_extra(manager, dataframe, seq_recs, seq_reservas, df_aux):
    df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
        .filter(dataframe.seq_reserva.isin(seq_reservas))

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_con", "ind_tipo_regimen_fac", "imp_margen_canco",
                "imp_venta", "imp_coste", "pct_impuesto_vta", "pct_impuesto_cpa", "imp_margen_canal", "ord_extra")

    dataframe = dataframe.withColumn("impuesto_canco",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                                            / 100)))))
                                     .otherwise((dataframe.imp_coste + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_coste) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.na.fill({'impuesto_canco': 0})

    # FILTER WHERE IN
    df_bok = manager.get_dataframe(tables['dwc_bok_t_extra'])
    df_discount = manager.get_dataframe(tables['dwc_cli_dir_t_cd_discount_bond'])
    df_campaign = manager.get_dataframe(tables['dwc_cli_dir_t_cd_campaign'])

    df_campaign = df_campaign.filter(df_campaign.ind_rentabilidad == 'N')

    df_filter = df_bok.join(df_discount, [df_bok.num_bono == df_discount.num_bono,
                                          df_bok.cod_interface == df_discount.cod_interface]) \
        .join(df_campaign, [df_discount.cod_campana == df_campaign.cod_campana,
                            df_discount.cod_interface == df_campaign.cod_interface]).select("grec_seq_rec",
                                                                                            "rres_seq_reserva",
                                                                                            "ord_extra")

    df_aux_filter = dataframe.join(df_filter, (dataframe.seq_rec == df_filter.grec_seq_rec) &
                                   (dataframe.seq_reserva == df_filter.rres_seq_reserva) &
                                   (dataframe.ord_extra == df_filter.ord_extra), 'left_outer') \
        .select(df_filter.grec_seq_rec, df_filter.rres_seq_reserva, df_filter.ord_extra) \
        .filter((df_filter.grec_seq_rec.isNull() == False) &
                (df_filter.rres_seq_reserva.isNull() == False) &
                (df_filter.ord_extra.isNull() == False))

    dataframe = dataframe.join(df_aux_filter, (dataframe.seq_rec != df_aux_filter.grec_seq_rec) &
                               (dataframe.seq_reserva != df_aux_filter.rres_seq_reserva) &
                               (dataframe.ord_extra != df_aux_filter.ord_extra),
                               'left_outer') \
        .drop(df_aux_filter.grec_seq_rec).drop(df_aux_filter.rres_seq_reserva).drop(df_aux_filter.ord_extra)

    dataframe = dataframe.na.fill({'impuesto_canco': 0})

    # join with the main ids "df_fields"
    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.grec_seq_rec,
                                        dataframe.seq_reserva == df_aux.seq_reserva]).drop(df_aux.seq_reserva)

    dataframe = dataframe.select("grec_seq_rec", "seq_reserva", "impuesto_canco")

    dataframe = dataframe.groupBy("grec_seq_rec", "seq_reserva").agg({'impuesto_canco': 'sum'}) \
        .withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    return dataframe


def sub_tax_cost_transfer_pricing(manager, df_fields, seq_recs, seq_reservas):
    df_hotel = manager.get_dataframe(tables["dwc_bok_t_canco_hotel"])
    df_circuit = manager.get_dataframe(tables["dwc_bok_t_canco_hotel_circuit"])
    df_other = manager.get_dataframe(tables["dwc_bok_t_canco_other"])
    df_transfer = manager.get_dataframe(tables["dwc_bok_t_canco_transfer"])
    df_endow = manager.get_dataframe(tables["dwc_bok_t_canco_endowments"])
    df_extra = manager.get_dataframe(tables["dwc_bok_t_canco_extra"])

    df_aux = df_fields.select("grec_seq_rec", "seq_reserva", "semp_cod_emp_rf", "fec_creacion", "gdiv_cod_divisa")

    df_hotel = sub_tax_cost_transfer_pricing_aux(manager, df_hotel, seq_recs, seq_reservas, df_aux)
    df_circuit = sub_tax_cost_transfer_pricing_aux(manager, df_circuit, seq_recs, seq_reservas, df_aux)
    df_other = sub_tax_cost_transfer_pricing_aux(manager, df_other, seq_recs, seq_reservas, df_aux)
    df_transfer = sub_tax_cost_transfer_pricing_aux(manager, df_transfer, seq_recs, seq_reservas, df_aux)
    df_endow = sub_tax_cost_transfer_pricing_aux(manager, df_endow, seq_recs, seq_reservas, df_aux)
    df_extra = sub_tax_cost_transfer_pricing_aux_extra(manager, df_extra, seq_recs, seq_reservas, df_aux)

    df_impuesto_canco = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(df_extra)

    df_impuesto_canco = df_impuesto_canco.groupBy("grec_seq_rec", "seq_reserva") \
        .agg({'impuesto_canco': 'sum'}).withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    df_impuesto_canco = df_impuesto_canco.withColumnRenamed("grec_seq_rec", "seq_rec") \
        .withColumnRenamed("seq_reserva", "seq_res")

    # add impuesto_canco
    df_fields = df_fields.join(df_impuesto_canco, [df_fields.grec_seq_rec == df_impuesto_canco.seq_rec,
                                                   df_fields.seq_reserva == df_impuesto_canco.seq_res],
                               'left_outer').drop("seq_rec", "seq_res")

    df_addcanco = sub_transfer_pricing_aux_add_canco(manager, df_fields, seq_recs, seq_reservas)

    df_addcanco = df_addcanco.withColumnRenamed("grec_seq_rec", "seq_rec") \
        .withColumnRenamed("seq_reserva", "seq_res")

    # add add_impuesto_canco
    df_fields = df_fields.join(df_addcanco, [df_fields.grec_seq_rec == df_addcanco.seq_rec,
                                             df_fields.seq_reserva == df_addcanco.seq_res],
                               "left_outer").drop(df_addcanco.seq_rec).drop(df_addcanco.seq_res)

    df_fields = df_fields.na.fill({'impuesto_canco': 0, 'add_impuesto_canco': 0})

    df_fields = df_fields.withColumn("Tax_Cost_Transfer_pricing",
                                     df_fields.impuesto_canco + df_fields.add_impuesto_canco) \
        .drop("impuesto_canco", "add_impuesto_canco")

    df_fields = df_fields.withColumn("Tax_Cost_Transfer_pricing", udf_round_ccy(df_fields.Tax_Cost_Transfer_pricing,
                                                                                df_fields.gdiv_cod_divisa))

    return df_fields


def sub_tax_sales_transfer_pricing_eur_aux(manager, dataframe, seq_recs, seq_reservas, df_aux):
    df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
        .filter(dataframe.seq_reserva.isin(seq_reservas))

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_fac", "imp_margen_canal",
                "imp_venta", "pct_impuesto_vta", "pct_impuesto_cpa")

    dataframe = dataframe.withColumn("amount",
                                     func.when(dataframe.ind_tipo_regimen_fac == 'E',
                                               dataframe.imp_margen_canal * (1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                                            / 100)))))
                                     .otherwise(dataframe.imp_venta * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_venta - dataframe.imp_margen_canal) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.grec_seq_rec,
                                        dataframe.seq_reserva == df_aux.seq_reserva]).drop(df_aux.seq_reserva)

    dataframe = dataframe.na.fill({'amount': 0})

    dataframe = dataframe.withColumn('impuesto_canal', udf_currency_exchange(dataframe.semp_cod_emp_rf,
                                                                             func.lit(EXCHANGE_RES),
                                                                             dataframe.fec_creacion,
                                                                             dataframe.gdiv_cod_divisa,
                                                                             dataframe.amount,
                                                                             func.lit(EUR)))

    dataframe = dataframe.select("grec_seq_rec", "seq_reserva", "impuesto_canal")

    dataframe = dataframe.groupBy("grec_seq_rec", "seq_reserva").agg({'impuesto_canal': 'sum'}) \
        .withColumnRenamed("SUM(impuesto_canal)", "impuesto_canal")

    return dataframe


def sub_tax_sales_transfer_pricing_eur_aux_extra(manager, dataframe, seq_recs, seq_reservas, df_aux):
    df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
        .filter(dataframe.seq_reserva.isin(seq_reservas))

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_fac", "imp_margen_canal",
                "imp_venta", "pct_impuesto_vta", "pct_impuesto_cpa", "ord_extra")

    dataframe = dataframe.withColumn("amount",
                                     func.when(dataframe.ind_tipo_regimen_fac == 'E',
                                               dataframe.imp_margen_canal * (1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                                            / 100)))))
                                     .otherwise(dataframe.imp_venta * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_venta - dataframe.imp_margen_canal) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    # dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.grec_seq_rec,
    #                                     dataframe.seq_reserva == df_aux.seq_reserva]).drop(df_aux.seq_reserva)

    # FILTER WHERE IN
    df_bok = manager.get_dataframe(tables['dwc_bok_t_extra'])
    df_discount = manager.get_dataframe(tables['dwc_cli_dir_t_cd_discount_bond'])
    df_campaign = manager.get_dataframe(tables['dwc_cli_dir_t_cd_campaign'])

    df_campaign = df_campaign.filter(df_campaign.ind_rentabilidad == 'N')

    df_filter = df_bok.join(df_discount, [df_bok.num_bono == df_discount.num_bono,
                                          df_bok.cod_interface == df_discount.cod_interface]) \
        .join(df_campaign, [df_discount.cod_campana == df_campaign.cod_campana,
                            df_discount.cod_interface == df_campaign.cod_interface]).select("grec_seq_rec",
                                                                                            "rres_seq_reserva",
                                                                                            "ord_extra")

    df_aux_filter = dataframe.join(df_filter, (dataframe.seq_rec == df_filter.grec_seq_rec) &
                                   (dataframe.seq_reserva == df_filter.rres_seq_reserva) &
                                   (dataframe.ord_extra == df_filter.ord_extra), 'left_outer') \
        .select(df_filter.grec_seq_rec, df_filter.rres_seq_reserva, df_filter.ord_extra) \
        .filter((df_filter.grec_seq_rec.isNull() == False) &
                (df_filter.rres_seq_reserva.isNull() == False) &
                (df_filter.ord_extra.isNull() == False))

    dataframe = dataframe.join(df_aux_filter, (dataframe.seq_rec != df_aux_filter.grec_seq_rec) &
                               (dataframe.seq_reserva != df_aux_filter.rres_seq_reserva) &
                               (dataframe.ord_extra != df_aux_filter.ord_extra),
                               'left_outer') \
        .drop(df_aux_filter.grec_seq_rec).drop(df_aux_filter.rres_seq_reserva).drop(df_aux_filter.ord_extra)

    dataframe = dataframe.na.fill({'amount': 0})

    # join with the main ids "df_fields"
    dataframe = dataframe.join(df_aux, (dataframe.seq_rec == df_aux.grec_seq_rec) &
                               (dataframe.seq_reserva == df_aux.seq_reserva)).drop(df_aux.seq_reserva)

    dataframe = dataframe.withColumn('impuesto_canal', udf_currency_exchange(dataframe.semp_cod_emp_rf,
                                                                             func.lit(EXCHANGE_RES),
                                                                             dataframe.fec_creacion,
                                                                             dataframe.gdiv_cod_divisa,
                                                                             dataframe.amount,
                                                                             func.lit(EUR)))

    dataframe = dataframe.select("grec_seq_rec", "seq_reserva", "impuesto_canal")

    dataframe = dataframe.groupBy("grec_seq_rec", "seq_reserva").agg({'impuesto_canal': 'sum'}) \
        .withColumnRenamed("SUM(impuesto_canal)", "impuesto_canal")

    return dataframe


def sub_tax_sales_transfer_pricing_eur(manager, df_fields, seq_recs, seq_reservas):
    df_hotel = manager.get_dataframe(tables["dwc_bok_t_canco_hotel"])
    df_circuit = manager.get_dataframe(tables["dwc_bok_t_canco_hotel_circuit"])
    df_other = manager.get_dataframe(tables["dwc_bok_t_canco_other"])
    df_transfer = manager.get_dataframe(tables["dwc_bok_t_canco_transfer"])
    df_endow = manager.get_dataframe(tables["dwc_bok_t_canco_endowments"])
    df_extra = manager.get_dataframe(tables["dwc_bok_t_canco_extra"])

    df_aux = df_fields.select("grec_seq_rec", "seq_reserva", "semp_cod_emp_rf", "fec_creacion", "gdiv_cod_divisa")

    df_hotel = sub_tax_sales_transfer_pricing_eur_aux(manager, df_hotel, seq_recs, seq_reservas, df_aux)
    df_circuit = sub_tax_sales_transfer_pricing_eur_aux(manager, df_circuit, seq_recs, seq_reservas, df_aux)
    df_other = sub_tax_sales_transfer_pricing_eur_aux(manager, df_other, seq_recs, seq_reservas, df_aux)
    df_transfer = sub_tax_sales_transfer_pricing_eur_aux(manager, df_transfer, seq_recs, seq_reservas, df_aux)
    df_endow = sub_tax_sales_transfer_pricing_eur_aux(manager, df_endow, seq_recs, seq_reservas, df_aux)
    df_extra = sub_tax_sales_transfer_pricing_eur_aux_extra(manager, df_extra, seq_recs, seq_reservas, df_aux)

    df_impuesto_canal = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(df_extra)

    df_impuesto_canal = df_impuesto_canal.groupBy("grec_seq_rec", "seq_reserva") \
        .agg({'impuesto_canal': 'sum'}).withColumnRenamed("SUM(impuesto_canal)", "impuesto_canal")

    df_impuesto_canal = df_impuesto_canal.withColumnRenamed("grec_seq_rec", "seq_rec") \
        .withColumnRenamed("seq_reserva", "seq_res")

    df_fields = df_fields.join(df_impuesto_canal, [df_fields.grec_seq_rec == df_impuesto_canal.seq_rec,
                                                   df_fields.seq_reserva == df_impuesto_canal.seq_res],
                               "left_outer").drop(df_impuesto_canal.seq_rec).drop(df_impuesto_canal.seq_res)

    df_fields = df_fields.na.fill({'impuesto_canal': 0}).withColumnRenamed('impuesto_canal',
                                                                           'Tax_Sales_Transfer_pricing_EUR')

    return df_fields


def sub_tax_transfer_pricing_eur_aux(manager, dataframe, seq_recs, seq_reservas, df_aux):
    df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
        .filter(dataframe.seq_reserva.isin(seq_reservas))

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_fac", "imp_margen_canal", "ind_tipo_regimen_con",
                "imp_coste", "imp_margen_canco", "imp_venta", "pct_impuesto_vta", "pct_impuesto_cpa")

    dataframe = dataframe.withColumn("amount1",
                                     func.when(dataframe.ind_tipo_regimen_fac == 'E',
                                               dataframe.imp_margen_canal * (1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                                            / 100)))))
                                     .otherwise(dataframe.imp_venta * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_venta - dataframe.imp_margen_canal) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.withColumn("amount2",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                                            / 100)))))
                                     .otherwise((dataframe.imp_coste + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_coste) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.na.fill({'amount1': 0})
    dataframe = dataframe.na.fill({'amount2': 0})

    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.grec_seq_rec,
                                        dataframe.seq_reserva == df_aux.seq_reserva]).drop(df_aux.seq_reserva)

    dataframe = dataframe.withColumn("impuesto_canco1", udf_currency_exchange(dataframe.semp_cod_emp_rf,
                                                                              func.lit(EXCHANGE_RES),
                                                                              dataframe.fec_creacion,
                                                                              dataframe.gdiv_cod_divisa,
                                                                              dataframe.amount1,
                                                                              func.lit(EUR)))

    dataframe = dataframe.withColumn("impuesto_canco2", udf_currency_exchange(dataframe.semp_cod_emp_rf,
                                                                              func.lit(EXCHANGE_RES),
                                                                              dataframe.fec_creacion,
                                                                              dataframe.gdiv_cod_divisa,
                                                                              dataframe.amount1,
                                                                              func.lit(EUR)))

    dataframe = dataframe.withColumn("impuesto_canco", dataframe.impuesto_canco1 + dataframe.impuesto_canco2)

    dataframe = dataframe.na.fill({'impuesto_canco': 0})

    dataframe = dataframe.select("grec_seq_rec", "seq_reserva", "impuesto_canco")

    dataframe = dataframe.groupBy("grec_seq_rec", "seq_reserva").agg({'impuesto_canco': 'sum'}). \
        withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    return dataframe


def sub_tax_transfer_pricing_eur_aux_extra(manager, dataframe, seq_recs, seq_reservas, df_aux):
    df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
        .filter(dataframe.seq_reserva.isin(seq_reservas))

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_fac", "imp_margen_canal", "ind_tipo_regimen_con",
                "imp_coste", "imp_margen_canco", "imp_venta", "pct_impuesto_vta", "pct_impuesto_cpa", "ord_extra")

    dataframe = dataframe.withColumn("amount1",
                                     func.when(dataframe.ind_tipo_regimen_fac == 'E',
                                               dataframe.imp_margen_canal * (1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                                            / 100)))))
                                     .otherwise(dataframe.imp_venta * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_venta - dataframe.imp_margen_canal) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.withColumn("amount2",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                                            / 100)))))
                                     .otherwise((dataframe.imp_coste + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_coste) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.na.fill({'amount1': 0})
    dataframe = dataframe.na.fill({'amount2': 0})

    # FILTER WHERE IN
    df_bok = manager.get_dataframe(tables['dwc_bok_t_extra'])
    df_discount = manager.get_dataframe(tables['dwc_cli_dir_t_cd_discount_bond'])
    df_campaign = manager.get_dataframe(tables['dwc_cli_dir_t_cd_campaign'])

    df_campaign = df_campaign.filter(df_campaign.ind_rentabilidad == 'N')

    df_filter = df_bok.join(df_discount, [df_bok.num_bono == df_discount.num_bono,
                                          df_bok.cod_interface == df_discount.cod_interface]) \
        .join(df_campaign, [df_discount.cod_campana == df_campaign.cod_campana,
                            df_discount.cod_interface == df_campaign.cod_interface]).select("grec_seq_rec",
                                                                                            "rres_seq_reserva",
                                                                                            "ord_extra")

    df_aux_filter = dataframe.join(df_filter, (dataframe.seq_rec == df_filter.grec_seq_rec) &
                                   (dataframe.seq_reserva == df_filter.rres_seq_reserva) &
                                   (dataframe.ord_extra == df_filter.ord_extra), 'left_outer') \
        .select(df_filter.grec_seq_rec, df_filter.rres_seq_reserva, df_filter.ord_extra) \
        .filter((df_filter.grec_seq_rec.isNull() == False) &
                (df_filter.rres_seq_reserva.isNull() == False) &
                (df_filter.ord_extra.isNull() == False))

    dataframe = dataframe.join(df_aux_filter, (dataframe.seq_rec != df_aux_filter.grec_seq_rec) &
                               (dataframe.seq_reserva != df_aux_filter.rres_seq_reserva) &
                               (dataframe.ord_extra != df_aux_filter.ord_extra),
                               'left_outer') \
        .drop(df_aux_filter.grec_seq_rec).drop(df_aux_filter.rres_seq_reserva).drop(df_aux_filter.ord_extra)

    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.grec_seq_rec,
                                        dataframe.seq_reserva == df_aux.seq_reserva]).drop(df_aux.seq_reserva)

    dataframe = dataframe.withColumn("impuesto_canco1", udf_currency_exchange(dataframe.semp_cod_emp_rf,
                                                                              func.lit(EXCHANGE_RES),
                                                                              dataframe.fec_creacion,
                                                                              dataframe.gdiv_cod_divisa,
                                                                              dataframe.amount1,
                                                                              func.lit(EUR)))

    dataframe = dataframe.withColumn("impuesto_canco2", udf_currency_exchange(dataframe.semp_cod_emp_rf,
                                                                              func.lit(EXCHANGE_RES),
                                                                              dataframe.fec_creacion,
                                                                              dataframe.gdiv_cod_divisa,
                                                                              dataframe.amount1,
                                                                              func.lit(EUR)))

    dataframe = dataframe.withColumn("impuesto_canco", dataframe.impuesto_canco1 + dataframe.impuesto_canco2)

    dataframe = dataframe.na.fill({'impuesto_canco': 0})

    dataframe = dataframe.select("grec_seq_rec", "seq_reserva", "impuesto_canco")

    dataframe = dataframe.groupBy("grec_seq_rec", "seq_reserva").agg({'impuesto_canco': 'sum'}). \
        withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    return dataframe


def sub_tax_transfer_pricing_eur(manager, df_fields, seq_recs, seq_reservas):
    df_hotel = manager.get_dataframe(tables["dwc_bok_t_canco_hotel"])
    df_circuit = manager.get_dataframe(tables["dwc_bok_t_canco_hotel_circuit"])
    df_other = manager.get_dataframe(tables["dwc_bok_t_canco_other"])
    df_transfer = manager.get_dataframe(tables["dwc_bok_t_canco_transfer"])
    df_endow = manager.get_dataframe(tables["dwc_bok_t_canco_endowments"])
    df_extra = manager.get_dataframe(tables["dwc_bok_t_canco_extra"])

    df_aux = df_fields.select("grec_seq_rec", "seq_reserva", "semp_cod_emp_rf", "fec_creacion", "gdiv_cod_divisa")

    df_hotel = sub_tax_transfer_pricing_eur_aux(manager, df_hotel, seq_recs, seq_reservas, df_aux)
    df_circuit = sub_tax_transfer_pricing_eur_aux(manager, df_circuit, seq_recs, seq_reservas, df_aux)
    df_other = sub_tax_transfer_pricing_eur_aux(manager, df_other, seq_recs, seq_reservas, df_aux)
    df_transfer = sub_tax_transfer_pricing_eur_aux(manager, df_transfer, seq_recs, seq_reservas, df_aux)
    df_endow = sub_tax_transfer_pricing_eur_aux(manager, df_endow, seq_recs, seq_reservas, df_aux)
    df_extra = sub_tax_transfer_pricing_eur_aux_extra(manager, df_extra, seq_recs, seq_reservas, df_aux)

    df_impuesto_canco = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(df_extra)

    df_impuesto_canco = df_impuesto_canco.groupBy("grec_seq_rec", "seq_reserva") \
        .agg({'impuesto_canco': 'sum'}).withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    df_impuesto_canco = df_impuesto_canco.withColumnRenamed("grec_seq_rec", "seq_rec") \
        .withColumnRenamed("seq_reserva", "seq_res")

    df_fields = df_fields.join(df_impuesto_canco, [df_fields.grec_seq_rec == df_impuesto_canco.seq_rec,
                                                   df_fields.seq_reserva == df_impuesto_canco.seq_res],
                               'left_outer').drop("seq_rec", "seq_res")

    df_addcanco = sub_transfer_pricing_aux_add_canco(manager, df_fields, seq_recs, seq_reservas, EUR)

    df_addcanco = df_addcanco.withColumnRenamed("grec_seq_rec", "seq_rec") \
        .withColumnRenamed("seq_reserva", "seq_res")

    # add add_impuesto_canco
    df_fields = df_fields.join(df_addcanco, [df_fields.grec_seq_rec == df_addcanco.seq_rec,
                                             df_fields.seq_reserva == df_addcanco.seq_res],
                               "left_outer").drop(df_addcanco.seq_rec).drop(df_addcanco.seq_res)

    df_fields = df_fields.na.fill({'impuesto_canco': 0, 'add_impuesto_canco': 0})

    df_fields = df_fields.withColumn("Tax_Transfer_pricing_EUR",
                                     df_fields.impuesto_canco + df_fields.add_impuesto_canco) \
        .drop("impuesto_canco", "add_impuesto_canco")

    return df_fields


###############
def sub_tax_cost_transfer_pricing_eur(manager, df_fields, seq_recs, seq_reservas):
    return df_fields


def sub_tax_cost_transfer_pricing_eur_aux(manager, dataframe, seq_recs, seq_reservas, df_aux):
    df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
        .filter(dataframe.seq_reserva.isin(seq_reservas))

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_con",
                "imp_coste", "imp_margen_canco", "pct_impuesto_vta", "pct_impuesto_cpa")

    dataframe = dataframe.withColumn("amount",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                                            / 100)))))
                                     .otherwise((dataframe.imp_coste + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_coste) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.na.fill({'amount': 0})

    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.grec_seq_rec,
                                        dataframe.seq_reserva == df_aux.seq_reserva]).drop(df_aux.seq_reserva)

    dataframe = dataframe.withColumn("impuesto_canco", udf_currency_exchange(dataframe.semp_cod_emp_rf,
                                                                             func.lit(EXCHANGE_RES),
                                                                             dataframe.fec_creacion,
                                                                             dataframe.gdiv_cod_divisa,
                                                                             dataframe.amount,
                                                                             func.lit(EUR)))

    dataframe = dataframe.na.fill({'impuesto_canco': 0})

    dataframe = dataframe.select("grec_seq_rec", "seq_reserva", "impuesto_canco")

    dataframe = dataframe.groupBy("grec_seq_rec", "seq_reserva").agg({'impuesto_canco': 'sum'}). \
        withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    return dataframe


def sub_tax_cost_transfer_pricing_eur_aux_extra(manager, dataframe, seq_recs, seq_reservas, df_aux):
    df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
        .filter(dataframe.seq_reserva.isin(seq_reservas))

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_con",
                "imp_coste", "imp_margen_canco", "pct_impuesto_vta", "pct_impuesto_cpa", "ord_extra")

    dataframe = dataframe.withColumn("amount",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                                            / 100)))))
                                     .otherwise((dataframe.imp_coste + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_coste) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.na.fill({'amount': 0})

    # FILTER WHERE IN
    df_bok = manager.get_dataframe(tables['dwc_bok_t_extra'])
    df_discount = manager.get_dataframe(tables['dwc_cli_dir_t_cd_discount_bond'])
    df_campaign = manager.get_dataframe(tables['dwc_cli_dir_t_cd_campaign'])

    df_campaign = df_campaign.filter(df_campaign.ind_rentabilidad == 'N')

    df_filter = df_bok.join(df_discount, [df_bok.num_bono == df_discount.num_bono,
                                          df_bok.cod_interface == df_discount.cod_interface]) \
        .join(df_campaign, [df_discount.cod_campana == df_campaign.cod_campana,
                            df_discount.cod_interface == df_campaign.cod_interface]).select("grec_seq_rec",
                                                                                            "rres_seq_reserva",
                                                                                            "ord_extra")

    df_aux_filter = dataframe.join(df_filter, (dataframe.seq_rec == df_filter.grec_seq_rec) &
                                   (dataframe.seq_reserva == df_filter.rres_seq_reserva) &
                                   (dataframe.ord_extra == df_filter.ord_extra), 'left_outer') \
        .select(df_filter.grec_seq_rec, df_filter.rres_seq_reserva, df_filter.ord_extra) \
        .filter((df_filter.grec_seq_rec.isNull() == False) &
                (df_filter.rres_seq_reserva.isNull() == False) &
                (df_filter.ord_extra.isNull() == False))

    dataframe = dataframe.join(df_aux_filter, (dataframe.seq_rec != df_aux_filter.grec_seq_rec) &
                               (dataframe.seq_reserva != df_aux_filter.rres_seq_reserva) &
                               (dataframe.ord_extra != df_aux_filter.ord_extra),
                               'left_outer') \
        .drop(df_aux_filter.grec_seq_rec).drop(df_aux_filter.rres_seq_reserva).drop(df_aux_filter.ord_extra)

    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.grec_seq_rec,
                                        dataframe.seq_reserva == df_aux.seq_reserva]).drop(df_aux.seq_reserva)

    dataframe = dataframe.withColumn("impuesto_canco", udf_currency_exchange(dataframe.semp_cod_emp_rf,
                                                                             func.lit(EXCHANGE_RES),
                                                                             dataframe.fec_creacion,
                                                                             dataframe.gdiv_cod_divisa,
                                                                             dataframe.amount,
                                                                             func.lit(EUR)))

    dataframe = dataframe.na.fill({'impuesto_canco': 0})

    dataframe = dataframe.select("grec_seq_rec", "seq_reserva", "impuesto_canco")

    dataframe = dataframe.groupBy("grec_seq_rec", "seq_reserva").agg({'impuesto_canco': 'sum'}). \
        withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    return dataframe


def sub_tax_cost_transfer_pricing_eur(manager, df_fields, seq_recs, seq_reservas):
    df_hotel = manager.get_dataframe(tables["dwc_bok_t_canco_hotel"])
    df_circuit = manager.get_dataframe(tables["dwc_bok_t_canco_hotel_circuit"])
    df_other = manager.get_dataframe(tables["dwc_bok_t_canco_other"])
    df_transfer = manager.get_dataframe(tables["dwc_bok_t_canco_transfer"])
    df_endow = manager.get_dataframe(tables["dwc_bok_t_canco_endowments"])
    df_extra = manager.get_dataframe(tables["dwc_bok_t_canco_extra"])

    df_aux = df_fields.select("grec_seq_rec", "seq_reserva", "semp_cod_emp_rf", "fec_creacion", "gdiv_cod_divisa")

    df_hotel = sub_tax_transfer_pricing_eur_aux(manager, df_hotel, seq_recs, seq_reservas, df_aux)
    df_circuit = sub_tax_transfer_pricing_eur_aux(manager, df_circuit, seq_recs, seq_reservas, df_aux)
    df_other = sub_tax_transfer_pricing_eur_aux(manager, df_other, seq_recs, seq_reservas, df_aux)
    df_transfer = sub_tax_transfer_pricing_eur_aux(manager, df_transfer, seq_recs, seq_reservas, df_aux)
    df_endow = sub_tax_transfer_pricing_eur_aux(manager, df_endow, seq_recs, seq_reservas, df_aux)
    df_extra = sub_tax_transfer_pricing_eur_aux_extra(manager, df_extra, seq_recs, seq_reservas, df_aux)

    df_impuesto_canco = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(df_extra)

    df_impuesto_canco = df_impuesto_canco.groupBy("grec_seq_rec", "seq_reserva") \
        .agg({'impuesto_canco': 'sum'}).withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    df_impuesto_canco = df_impuesto_canco.withColumnRenamed("grec_seq_rec", "seq_rec") \
        .withColumnRenamed("seq_reserva", "seq_res")

    df_fields = df_fields.join(df_impuesto_canco, [df_fields.grec_seq_rec == df_impuesto_canco.seq_rec,
                                                   df_fields.seq_reserva == df_impuesto_canco.seq_res],
                               'left_outer').drop("seq_rec", "seq_res")

    df_addcanco = sub_transfer_pricing_aux_add_canco(manager, df_fields, seq_recs, seq_reservas, EUR)

    df_addcanco = df_addcanco.withColumnRenamed("grec_seq_rec", "seq_rec") \
        .withColumnRenamed("seq_reserva", "seq_res")

    # add add_impuesto_canco
    df_fields = df_fields.join(df_addcanco, [df_fields.grec_seq_rec == df_addcanco.seq_rec,
                                             df_fields.seq_reserva == df_addcanco.seq_res],
                               "left_outer").drop(df_addcanco.seq_rec).drop(df_addcanco.seq_res)

    df_fields = df_fields.na.fill({'impuesto_canco': 0, 'add_impuesto_canco': 0})

    df_fields = df_fields.withColumn("Tax_Transfer_pricing_EUR",
                                     df_fields.impuesto_canco + df_fields.add_impuesto_canco) \
        .drop("impuesto_canco", "add_impuesto_canco")

    return df_fields
