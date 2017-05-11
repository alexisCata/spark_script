import sys
from pyspark.sql import functions as func
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType, StringType
import psycopg2
from settings import DATABASE, HOST, PORT, USER, PASS
from functions import *


EUR = 'EUR'
udf_currency_exchange = func.udf(currency_exchange, DecimalType(15, 3))
udf_calculate_night_amount = func.udf(calculate_night_amount, DecimalType(15, 3))
# udf_round_ccy = func.udf(round_ccy, DecimalType())
udf_round_ccy = func.udf(round_ccy, StringType())  # The results have different number of decimals


def select_direct_fields(manager):
    connection = psycopg2.connect(dbname=DATABASE, host=HOST,
                                  port=PORT, user=USER, password=PASS)

    cursor = connection.cursor()

    select = "with hotel as" \
             " (select hv.grec_Seq_rec, hv.rres_seq_reserva, hv.ghor_seq_hotel, h.seq_hotel, h.izge_cod_destino, di1.ides_cod_destino, di1.nom_destino, di1.sidi_cod_idioma," \
             "     row_number () over (partition by hv.grec_Seq_rec, hv.rres_seq_reserva order by grec_Seq_rec, rres_seq_reserva, ghor_seq_hotel) as rownum" \
             "   from (select grec_Seq_rec, rres_seq_reserva, ghor_seq_hotel from hbgdwc.dwc_bok_t_hotel_sale) hv" \
             "     inner join (select seq_hotel, izge_cod_destino from hbgdwc.dwc_mtd_t_hotel) h on (h.seq_hotel = hv.ghor_seq_hotel)" \
             "     inner join (select ides_cod_destino, nom_destino, sidi_cod_idioma from hbgdwc.dwc_itn_t_internet_destination_id) di1 on (di1.ides_cod_destino = h.izge_cod_destino AND di1.sidi_cod_idioma  = 'ENG')" \
             " )," \
             " oth_con as" \
             " (select o.seq_rec_other, o.seq_reserva, o.nom_contrato, o.ind_tipo_otro, o.fec_desde_other, c.seq_rec, c.nom_contrato, c.cod_destino, c.ind_tipo_otro, c.fec_desde, c.fec_hasta, di2.ides_cod_Destino, di2.sidi_cod_idioma, di2.nom_destino," \
             "     row_number () over (partition by o.seq_rec_other, o.seq_reserva order by o.seq_rec_other, o.seq_reserva, o.nom_contrato) as rownum" \
             "   from (select seq_rec as seq_rec_other, seq_reserva, nom_contrato, ind_tipo_otro, fec_desde as fec_desde_other from hbgdwc.dwc_bok_t_other) o" \
             "     inner join (select seq_rec, nom_contrato, cod_destino, ind_tipo_otro, fec_desde, fec_hasta from hbgdwc.dwc_con_t_contract_other) c on (c.seq_rec = o.seq_rec_other AND c.nom_contrato = o.nom_contrato AND c.ind_tipo_otro = o.ind_tipo_otro AND o.fec_desde_other BETWEEN c.fec_desde AND c.fec_hasta)" \
             "     inner join (select ides_cod_Destino, sidi_cod_idioma, nom_destino from hbgdwc.dwc_itn_t_internet_destination_id) di2 on (di2.ides_cod_Destino = c.cod_destino AND di2.sidi_cod_idioma  = 'ENG')" \
             " )," \
             " reventa as" \
             " (select seq_rec, seq_reserva, ind_status from hbgdwc.dwc_cry_t_cancellation_recovery_res_resale" \
             " )," \
             " information as" \
             " (select i.seq_rec, i.seq_reserva, i.aplicacion," \
             "     row_number () over (partition by i.seq_rec, i.seq_reserva) as rownum" \
             "   from hbgdwc.dwc_bok_t_booking_information i" \
             "   where i.tipo_op='A'" \
             " )," \
             " num_services as" \
             " (SELECT ri.grec_seq_rec, ri.rres_seq_reserva, COUNT(1) act_services" \
             "   FROM hbgdwc.dwc_bok_t_hotel_sale ri" \
             "   WHERE ri.fec_cancelacion is null" \
             "   group by ri.grec_seq_rec, ri.rres_seq_reserva" \
             " )," \
             " cabecera as" \
             " (select grec_Seq_rec, seq_reserva, gdiv_cod_divisa booking_currency, fec_creacion creation_date, grec_seq_rec || '-' || seq_reserva interface_id, gtto_seq_ttoo, cod_pais_cliente, nro_ad, nro_ni," \
             "   gdiv_cod_divisa, fec_modifica, fec_cancelacion, fec_desde, fint_cod_interface, seq_rec_hbeds, fec_hasta, nom_general, ind_tipo_credito, gdep_cod_depart, rtre_cod_tipo_res, ind_facturable_res," \
             "   ind_facturable_adm, pct_comision, pct_rappel, ind_confirma, cod_divisa_p, seq_ttoo_p, cod_suc_p, seq_agencia_p, seq_sucursal_p, seq_rec_expediente, seq_res_expediente, ind_tippag" \
             "   from hbgdwc.dwc_bok_t_booking" \
             "   where fec_creacion BETWEEN '{}'::timestamp AND '{}'::timestamp" \
             "     and seq_reserva>0" \
             " )," \
            " ri as (SELECT ri.seq_rec, ri.seq_reserva, MIN(ri.fec_creacion) min_fec_creacion " \
             "                 FROM hbgdwc.dwc_bok_t_booking_information ri " \
             "                WHERE ri.tipo_op = 'A' " \
             "                group by ri.seq_rec, ri.seq_reserva) "\
             "select cabecera.grec_seq_rec grec_seq_rec, " \
             "       cabecera.seq_reserva seq_reserva, " \
             "       cabecera.gdiv_cod_divisa gdiv_cod_divisa, "\
             "       cabecera.creation_date fec_creacion, " \
             "       rf.semp_cod_emp semp_cod_emp_rf, " \
             "       cabecera.interface_id, " \
             "       re.semp_cod_emp operative_company, " \
             "       re.sofi_cod_ofi operative_office, " \
             "       re.des_receptivo operative_office_desc, " \
             "       cabecera.grec_seq_rec operative_incoming, " \
             "       cabecera.seq_reserva booking_id, " \
             "       cabecera.fint_cod_interface interface, " \
             "       rf.semp_cod_emp invoicing_company, " \
             "       rf.sofi_cod_ofi invoicing_office, " \
             "       rf.seq_rec invoicing_incoming, " \
             "       TRUNC (cabecera.creation_date) creation_date, " \
             "       cabecera.creation_date creation_ts, " \
             "       ri.min_fec_creacion first_booking_ts, " \
             "       NVL(TO_CHAR(TRUNC (cabecera.fec_modifica),'yyyy-mm-dd'),'') modification_date, " \
             "       NVL(TO_CHAR(cabecera.fec_modifica,'yyyy-mm-dd hh24:mi:ss'),'') modification_ts, " \
             "       NVL(TO_CHAR(TRUNC (cabecera.fec_cancelacion),'yyyy-mm-dd'),'') cancellation_date, " \
             "       NVL(TO_CHAR(cabecera.fec_cancelacion,'yyyy-mm-dd hh24:mi:ss'),'') cancellation_ts, " \
             "       decode(cabecera.fec_cancelacion, null, 'N', 'S') cancelled_booking, " \
             "       TRUNC(GREATEST(cabecera.creation_date, cabecera.fec_modifica, cabecera.fec_cancelacion)) status_date, " \
             "       trunc(cabecera.fec_desde) booking_service_from, " \
             "       trunc(cabecera.fec_hasta) booking_service_to, " \
             "       t.seq_ttoo client_code, " \
             "       t.nom_corto_ttoo customer_name, " \
             "       NVL (cabecera.cod_pais_cliente, t.gpai_cod_pais_mercado) source_market, " \
             "       p.cod_iso source_market_iso, " \
             "       REPLACE(cabecera.nom_general,';','') holder, " \
             "       cabecera.ind_tipo_credito credit_type, " \
             "       cabecera.nro_ad num_adults, " \
             "       cabecera.nro_ni num_childrens, " \
             "       cabecera.gdep_cod_depart department_code, " \
             "       cabecera.rtre_cod_tipo_res booking_type, " \
             "       cabecera.ind_facturable_res invoicing_booking, " \
             "       cabecera.ind_facturable_adm invoicing_admin, " \
             "       NVL(cabecera.pct_comision,0) Client_commision_esp, " \
             "       NVL(cabecera.pct_rappel,0) client_override_esp, " \
             "       cabecera.ind_confirma confirmed_booking, " \
             "       decode (i.partner_ttoo, null, 'N', 'S') Partner_booking, " \
             "       NVL(cabecera.cod_divisa_p,'') Partner_booking_currency, " \
             "       NVL(cabecera.seq_ttoo_p,0) Partner_code, " \
             "       NVL(cabecera.cod_suc_p,0) Partner_brand, " \
             "       NVl(cabecera.seq_agencia_p,0) Partner_agency_code, " \
             "       NVL(cabecera.seq_sucursal_p,0) Partner_agency_brand, " \
             "       NVL(cabecera.seq_rec_expediente,0) Booking_file_incoming, " \
             "       NVL(cabecera.seq_res_expediente,0) booking_file_number, " \
             "       decode (cabecera.ind_tippag, null, 'Merchant', 'Pago en hotel') Accomodation_model, " \
             "       NVL ( hotel.izge_cod_destino || '-' || hotel.nom_destino, oth_con.cod_destino || '-' ||oth_con.nom_destino, 'NO_DESTINATION_CODE'  ) Destination_code, " \
             "       cabecera.gdiv_cod_divisa booking_currency, " \
             "       nvl(ttv.ttv, 0) TTV_booking_currency, " \
             "       nvl(ttv.ttv, 0)*nvl(tip.rate, 0) TTV_EUR_currency, " \
             "       nvl(tax_ttv.tax_ttv, 0) tax_ttv, " \
             "       nvl(tax_ttv.tax_ttv, 0)*nvl(tip.rate, 0) tax_ttv_eur, " \
             "       nvl(tax_ttv.tax_ttv_toms, 0) tax_ttv_toms, " \
             "       nvl(tax_ttv.tax_ttv_toms, 0)*nvl(tip.rate, 0) Tax_TTV_EUR_TOMS, " \
             "       0 MISSING_CANCO_Tax_Sales_Transfer_pricing, " \
             "       0 MISSING_CANCO_Tax_Sales_Transfer_pricing_EUR, " \
             "       0 MISSING_CANCO_Transfer_pricing, " \
             "       0 missing_canco_tax_transfer_pricing_eur, " \
             "       0 MISSING_CANCO_Tax_Cost_Transfer_pricing, " \
             "       0 MISSING_CANCO_Tax_Cost_Transfer_pricing_EUR, " \
             "       nvl(cli_comm.cli_comm, 0) Client_Commision, " \
             "       nvl(cli_comm.cli_comm, 0)*nvl(tip.rate, 0) Client_EUR_Commision, " \
             "       nvl(tax_cli_comm.Tax_Client_Com, 0) Tax_Client_commision, " \
             "       nvl(tax_cli_comm.Tax_Client_Com, 0)*nvl(tip.rate, 0) Tax_Client_EUR_commision, " \
             "       nvl(cli_rappel.cli_rappel, 0) client_rappel, " \
             "       nvl(cli_rappel.cli_rappel, 0)*nvl(tip.rate, 0) Client_EUR_rappel, " \
             "       nvl(tax_cli_rappel.tax_cli_rappel, 0) tax_client_rappel, " \
             "       nvl(tax_cli_rappel.tax_cli_rappel, 0)*nvl(tip.rate, 0) tax_Client_EUR_rappel, " \
             "       0 as missing_cost_booking_currency,   " \
             "       nvl(booking_cost.booking_cost, 0) cost_eur_currency, " \
             "       0 as MISSING_tax_cost, " \
             "       nvl(tax_cost.tax_cost, 0) tax_cost_EUR, " \
             "       0 as MISSING_tax_cost_TOMS, " \
             "       nvl(tax_cost.tax_cost_TOMS, 0) tax_cost_EUR_TOMS, " \
             "       inf.aplicacion Application, " \
             "       decode( " \
             "              decode(rev.ind_status, " \
             "                      'RR', 'R', " \
             "                      'CN', 'RL', " \
             "                      'RL', decode(cabecera.fec_cancelacion, null, 'RS', 'C'), " \
             "                      rev.ind_status), " \
             "              'RS', 'Reventa', " \
             "              'C', 'Cancelada', " \
             "              'RL', 'Liberada', " \
             "              'R', 'Reventa') canrec_status, " \
             "       nvl(GSA_comm.GSA_Comm, 0) GSA_Commision, " \
             "       nvl(GSA_comm.GSA_Comm, 0)*nvl(tip.rate, 0) GSA_EUR_Commision, " \
             "       nvl(GSA_comm.Tax_GSA_Comm, 0) Tax_GSA_Commision, " \
             "       nvl(GSA_comm.Tax_GSA_Comm, 0)*nvl(tip.rate, 0) Tax_GSA_EUR_Commision, " \
             "       0 MISSING_Agency_commision_hotel_payment,  " \
             "       0 MISSING_Tax_Agency_commision_hotel_pay,  " \
             "       nvl(age_com_hot_pay.Agency_commision_hotel_payment_EUR, 0) agency_comm_hotel_pay_eur, " \
             "       nvl(age_com_hot_pay.Tax_Agency_commision_hotel_pay_EUR, 0) tax_agency_comm_hotel_pay_eur, " \
             "       0 MISSING_Fix_override_hotel_payment,  " \
             "       0 MISSING_Tax_Fix_override_hotel_pay,  " \
             "       nvl(fix_over_hot_pay.Fix_override_hotel_payment_EUR, 0) fix_override_hotel_pay_eur, " \
             "       nvl(fix_over_hot_pay.Tax_Fix_override_hotel_pay_EUR, 0) tax_fix_overr_hotel_pay_eur, " \
             "       0 MISSING_Var_override_hotel_payment,  " \
             "       0 MISSING_Tax_Var_override_hotel_pay,  " \
             "       nvl(var_over_hot_pay.Var_override_hotel_payment_EUR, 0) var_override_hotel_pay_eur, " \
             "       nvl(var_over_hot_pay.Tax_Var_override_hotel_pay_EUR, 0) Tax_Var_override_hotel_pay_EUR, " \
             "       0 MISSING_Hotel_commision_hotel_payment,  " \
             "       0 MISSING_Tax_Hotel_commision_Hotel_pay,  " \
             "       nvl(hot_comm_hot_pay.Hotel_commision_hotel_payment_EUR, 0) hotel_commision_hotel_pay_eur, " \
             "       nvl(hot_comm_hot_pay.Tax_Hotel_commision_Hotel_pay_EUR, 0) tax_hotel_comm_hotel_pay_eur, " \
             "       0 MISSING_Marketing_contribution,  " \
             "       0 MISSING_Tax_marketing_contribution,  " \
             "       nvl(marketing_contrib.Marketing_contribution_EUR, 0) Marketing_contribution_EUR, " \
             "       nvl(marketing_contrib.Tax_marketing_contribution_EUR, 0) Tax_marketing_contribution_EUR, " \
             "        " \
             "       0 MISSING_bank_expenses,  " \
             "       0 MISSING_Tax_Bank_expenses,  " \
             "       nvl(bank_exp.bank_expenses_EUR, 0) bank_expenses_EUR, " \
             "       nvl(bank_exp.Tax_Bank_expenses_EUR, 0) Tax_Bank_expenses_EUR, " \
             "       0 MISSING_Platform_Fee,  " \
             "       0 MISSING_Tax_Platform_fee,  " \
             "       nvl(platform_fee.Platform_Fee_EUR, 0) Platform_Fee_EUR, " \
             "       nvl(platform_fee.Tax_Platform_fee_EUR, 0) Tax_Platform_fee_EUR, " \
             "       0 MISSING_credit_card_fee,  " \
             "       0 missing_tax_credit_card_fee,   " \
             "       nvl(credit_card_fee.credit_card_fee_EUR, 0) credit_card_fee_EUR, " \
             "       nvl(credit_card_fee.Tax_credit_card_fee_EUR, 0) Tax_credit_card_fee_EUR, " \
             "       0 MISSING_Withholding,  " \
             "       0 MISSING_Tax_withholding,  " \
             "       nvl(withholding.Withholding_EUR, 0) Withholding_EUR, " \
             "       nvl(withholding.Tax_withholding_EUR, 0) Tax_withholding_EUR, " \
             "       0 MISSING_Local_Levy,  " \
             "       0 MISSING_Tax_Local_Levy,  " \
             "       nvl(local_levy.Local_Levy_EUR, 0) Local_Levy_EUR, " \
             "       nvl(local_levy.Tax_Local_Levy_EUR, 0) Tax_Local_Levy_EUR, " \
             "       0 MISSING_Partner_Third_commision,  " \
             "       0 MISSING_Tax_partner_third_commision,  " \
             "       nvl(partner_third_comm.Partner_Third_commision_EUR, 0) partner_third_comm_eur, " \
             "       nvl(partner_third_comm.Tax_partner_third_commision_EUR, 0) tax_partner_third_comm_eur, " \
             "       nvl(num_services.act_services, 0) NUMBER_ACTIVE_ACC_SERV " \
             "from " \
             " cabecera " \
             "inner join hbgdwc.dwc_mtd_t_ttoo t on  cabecera.gtto_seq_ttoo = t.seq_ttoo " \
             "inner join hbgdwc.dwc_gen_t_general_country p on (nvl(cabecera.cod_pais_cliente, t.gpai_cod_pais_mercado) = p.cod_pais) " \
             "left join hbgdwc.dwc_mtd_t_receptive re on cabecera.grec_Seq_rec  = re.seq_rec  " \
             "left join hbgdwc.dwc_mtd_t_receptive rf on cabecera.seq_rec_hbeds = rf.seq_rec  " \
             "left join hbgdwc.dwc_itf_t_fc_interface i on cabecera.fint_cod_interface = i.cod_interface       " \
             "left join reventa rev on (rev.seq_rec=cabecera.grec_seq_rec and rev.seq_reserva=cabecera.seq_reserva) " \
             "left join information inf on (inf.seq_rec=cabecera.grec_seq_rec and inf.seq_reserva=cabecera.seq_reserva and inf.rownum=1) " \
             "left join ri on ri.seq_rec = cabecera.grec_seq_rec AND ri.seq_reserva = cabecera.seq_reserva " \
             "left join hotel on (hotel.grec_Seq_rec = cabecera.grec_seq_rec AND hotel.rres_seq_reserva = cabecera.seq_reserva and hotel.rownum=1) " \
             "left join oth_con on (oth_con.seq_rec_other = cabecera.grec_seq_rec AND oth_con.seq_reserva = cabecera.seq_reserva and oth_con.rownum=1) " \
             "left join num_services on (num_services.grec_Seq_rec = cabecera.grec_seq_rec AND num_services.rres_seq_reserva = cabecera.seq_reserva) " \
             "left join hbgdwc.svd_david_ttv ttv on ttv.grec_seq_rec||'-'||ttv.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_tax_ttv tax_ttv on tax_ttv.grec_seq_rec||'-'||tax_ttv.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_client_com cli_comm on cli_comm.grec_seq_rec||'-'||cli_comm.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_tax_client_com tax_cli_comm on tax_cli_comm.grec_seq_rec||'-'||tax_cli_comm.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_client_rap cli_rappel on cli_rappel.grec_seq_rec||'-'||cli_rappel.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_tax_client_rap tax_cli_rappel on tax_cli_rappel.grec_seq_rec||'-'||tax_cli_rappel.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_cost booking_cost on booking_cost.grec_seq_rec||'-'||booking_cost.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_tax_cost tax_cost on tax_cost.grec_seq_rec||'-'||tax_cost.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_GSA_comm GSA_comm on GSA_comm.seq_rec||'-'||GSA_comm.seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_age_comm_hot_pay age_com_hot_pay on age_com_hot_pay.grec_seq_rec||'-'||age_com_hot_pay.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_fix_over_hot_pay fix_over_hot_pay on fix_over_hot_pay.grec_seq_rec||'-'||fix_over_hot_pay.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_var_over_hot_pay var_over_hot_pay on var_over_hot_pay.grec_seq_rec||'-'||var_over_hot_pay.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_hot_comm_hot_pay hot_comm_hot_pay on hot_comm_hot_pay.grec_seq_rec||'-'||hot_comm_hot_pay.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_marketing_contrib marketing_contrib on marketing_contrib.grec_seq_rec||'-'||marketing_contrib.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_bank_exp bank_exp on bank_exp.grec_seq_rec||'-'||bank_exp.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_platform_fee platform_fee on platform_fee.grec_seq_rec||'-'||platform_fee.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_credit_card_fee credit_card_fee on credit_card_fee.grec_seq_rec||'-'||credit_card_fee.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_withholding withholding on withholding.grec_seq_rec||'-'||withholding.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_local_levy local_levy on local_levy.grec_seq_rec||'-'||local_levy.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_partner_third_comm partner_third_comm on partner_third_comm.grec_seq_rec||'-'||partner_third_comm.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.david_tasas_cambio_flc tip on trunc(cabecera.creation_date)=tip.date and cabecera.booking_currency=tip.currency ".format(
        sys.argv[1], sys.argv[2])

    # select = "select cabecera.grec_seq_rec grec_seq_rec, " \
    #          "       cabecera.seq_reserva seq_reserva, " \
    #          "       cabecera.gdiv_cod_divisa gdiv_cod_divisa, "\
    #          "       cabecera.fec_creacion fec_creacion, " \
    #          "       rf.semp_cod_emp semp_cod_emp_rf, " \
    #          "       cabecera.interface_id, " \
    #          "       re.semp_cod_emp operative_company, " \
    #          "       re.sofi_cod_ofi operative_office, " \
    #          "       re.des_receptivo operative_office_desc, " \
    #          "       cabecera.grec_seq_rec operative_incoming, " \
    #          "       cabecera.seq_reserva booking_id, " \
    #          "       cabecera.fint_cod_interface interface, " \
    #          "       rf.semp_cod_emp invoicing_company, " \
    #          "       rf.sofi_cod_ofi invoicing_office, " \
    #          "       rf.seq_rec invoicing_incoming, " \
    #          "       TRUNC (cabecera.creation_date) creation_date, " \
    #          "       cabecera.creation_date creation_ts, " \
    #          "       ri.min_fec_creacion first_booking_ts, " \
    #          "       NVL(TO_CHAR(TRUNC (cabecera.fec_modifica),'yyyy-mm-dd'),'') modification_date, " \
    #          "       NVL(TO_CHAR(cabecera.fec_modifica,'yyyy-mm-dd hh24:mi:ss'),'') modification_ts, " \
    #          "       NVL(TO_CHAR(TRUNC (cabecera.fec_cancelacion),'yyyy-mm-dd'),'') cancellation_date, " \
    #          "       NVL(TO_CHAR(cabecera.fec_cancelacion,'yyyy-mm-dd hh24:mi:ss'),'') cancellation_ts, " \
    #          "       decode(cabecera.fec_cancelacion, null, 'N', 'S') cancelled_booking, " \
    #          "       TRUNC(GREATEST(cabecera.creation_date, cabecera.fec_modifica, cabecera.fec_cancelacion)) status_date, " \
    #          "       trunc(cabecera.fec_desde) booking_service_from, " \
    #          "       trunc(cabecera.fec_hasta) booking_service_to, " \
    #          "       t.seq_ttoo client_code, " \
    #          "       t.nom_corto_ttoo customer_name, " \
    #          "       NVL (cabecera.cod_pais_cliente, t.gpai_cod_pais_mercado) source_market, " \
    #          "       p.cod_iso source_market_iso, " \
    #          "       REPLACE(cabecera.nom_general,';','') holder, " \
    #          "       cabecera.ind_tipo_credito credit_type, " \
    #          "       cabecera.nro_ad num_adults, " \
    #          "       cabecera.nro_ni num_childrens, " \
    #          "       cabecera.gdep_cod_depart department_code, " \
    #          "       cabecera.rtre_cod_tipo_res booking_type, " \
    #          "       cabecera.ind_facturable_res invoicing_booking, " \
    #          "       cabecera.ind_facturable_adm invoicing_admin, " \
    #          "       NVL(cabecera.pct_comision,0) Client_commision_esp, " \
    #          "       NVL(cabecera.pct_rappel,0) client_override_esp, " \
    #          "       cabecera.ind_confirma confirmed_booking, " \
    #          "       decode (i.partner_ttoo, null, 'N', 'S') Partner_booking, " \
    #          "       NVL(cabecera.cod_divisa_p,'') Partner_booking_currency, " \
    #          "       NVL(cabecera.seq_ttoo_p,0) Partner_code, " \
    #          "       NVL(cabecera.cod_suc_p,0) Partner_brand, " \
    #          "       NVl(cabecera.seq_agencia_p,0) Partner_agency_code, " \
    #          "       NVL(cabecera.seq_sucursal_p,0) Partner_agency_brand, " \
    #          "       NVL(cabecera.seq_rec_expediente,0) Booking_file_incoming, " \
    #          "       NVL(cabecera.seq_res_expediente,0) booking_file_number, " \
    #          "       decode (cabecera.ind_tippag, null, 'Merchant', 'Pago en hotel') Accomodation_model, " \
    #          "       NVL ( hotel.izge_cod_destino || '-' || hotel.nom_destino, oth_con.cod_destino || '-' ||oth_con.nom_destino, 'NO_DESTINATION_CODE'  ) Destination_code, " \
    #          "       cabecera.gdiv_cod_divisa booking_currency, " \
    #          "       nvl(ttv.ttv, 0) TTV_booking_currency, " \
    #          "       nvl(ttv.ttv, 0)*nvl(tip.rate, 0) TTV_EUR_currency, " \
    #          "       nvl(tax_ttv.tax_ttv, 0) tax_ttv, " \
    #          "       nvl(tax_ttv.tax_ttv, 0)*nvl(tip.rate, 0) tax_ttv_eur, " \
    #          "       nvl(tax_ttv.tax_ttv_toms, 0) tax_ttv_toms, " \
    #          "       nvl(tax_ttv.tax_ttv_toms, 0)*nvl(tip.rate, 0) Tax_TTV_EUR_TOMS, " \
    #          "       0 MISSING_CANCO_Tax_Sales_Transfer_pricing, " \
    #          "       0 MISSING_CANCO_Tax_Sales_Transfer_pricing_EUR, " \
    #          "       0 MISSING_CANCO_Transfer_pricing, " \
    #          "       0 missing_canco_tax_transfer_pricing_eur, " \
    #          "       0 MISSING_CANCO_Tax_Cost_Transfer_pricing, " \
    #          "       0 MISSING_CANCO_Tax_Cost_Transfer_pricing_EUR, " \
    #          "       nvl(cli_comm.cli_comm, 0) Client_Commision, " \
    #          "       nvl(cli_comm.cli_comm, 0)*nvl(tip.rate, 0) Client_EUR_Commision, " \
    #          "       nvl(tax_cli_comm.Tax_Client_Com, 0) Tax_Client_commision, " \
    #          "       nvl(tax_cli_comm.Tax_Client_Com, 0)*nvl(tip.rate, 0) Tax_Client_EUR_commision, " \
    #          "       nvl(cli_rappel.cli_rappel, 0) client_rappel, " \
    #          "       nvl(cli_rappel.cli_rappel, 0)*nvl(tip.rate, 0) Client_EUR_rappel, " \
    #          "       nvl(tax_cli_rappel.tax_cli_rappel, 0) tax_client_rappel, " \
    #          "       nvl(tax_cli_rappel.tax_cli_rappel, 0)*nvl(tip.rate, 0) tax_Client_EUR_rappel, " \
    #          "       0 as missing_cost_booking_currency,   " \
    #          "       nvl(booking_cost.booking_cost, 0) cost_eur_currency, " \
    #          "       0 as MISSING_tax_cost, " \
    #          "       nvl(tax_cost.tax_cost, 0) tax_cost_EUR, " \
    #          "       0 as MISSING_tax_cost_TOMS, " \
    #          "       nvl(tax_cost.tax_cost_TOMS, 0) tax_cost_EUR_TOMS, " \
    #          "       inf.aplicacion Application, " \
    #          "       decode( " \
    #          "              decode(rev.ind_status, " \
    #          "                      'RR', 'R', " \
    #          "                      'CN', 'RL', " \
    #          "                      'RL', decode(cabecera.fec_cancelacion, null, 'RS', 'C'), " \
    #          "                      rev.ind_status), " \
    #          "              'RS', 'Reventa', " \
    #          "              'C', 'Cancelada', " \
    #          "              'RL', 'Liberada', " \
    #          "              'R', 'Reventa') canrec_status, " \
    #          "       nvl(GSA_comm.GSA_Comm, 0) GSA_Commision, " \
    #          "       nvl(GSA_comm.GSA_Comm, 0)*nvl(tip.rate, 0) GSA_EUR_Commision, " \
    #          "       nvl(GSA_comm.Tax_GSA_Comm, 0) Tax_GSA_Commision, " \
    #          "       nvl(GSA_comm.Tax_GSA_Comm, 0)*nvl(tip.rate, 0) Tax_GSA_EUR_Commision, " \
    #          "       0 MISSING_Agency_commision_hotel_payment,  " \
    #          "       0 MISSING_Tax_Agency_commision_hotel_pay,  " \
    #          "       nvl(age_com_hot_pay.Agency_commision_hotel_payment_EUR, 0) agency_comm_hotel_pay_eur, " \
    #          "       nvl(age_com_hot_pay.Tax_Agency_commision_hotel_pay_EUR, 0) tax_agency_comm_hotel_pay_eur, " \
    #          "       0 MISSING_Fix_override_hotel_payment,  " \
    #          "       0 MISSING_Tax_Fix_override_hotel_pay,  " \
    #          "       nvl(fix_over_hot_pay.Fix_override_hotel_payment_EUR, 0) fix_override_hotel_pay_eur, " \
    #          "       nvl(fix_over_hot_pay.Tax_Fix_override_hotel_pay_EUR, 0) tax_fix_overr_hotel_pay_eur, " \
    #          "       0 MISSING_Var_override_hotel_payment,  " \
    #          "       0 MISSING_Tax_Var_override_hotel_pay,  " \
    #          "       nvl(var_over_hot_pay.Var_override_hotel_payment_EUR, 0) var_override_hotel_pay_eur, " \
    #          "       nvl(var_over_hot_pay.Tax_Var_override_hotel_pay_EUR, 0) Tax_Var_override_hotel_pay_EUR, " \
    #          "       0 MISSING_Hotel_commision_hotel_payment,  " \
    #          "       0 MISSING_Tax_Hotel_commision_Hotel_pay,  " \
    #          "       nvl(hot_comm_hot_pay.Hotel_commision_hotel_payment_EUR, 0) hotel_commision_hotel_pay_eur, " \
    #          "       nvl(hot_comm_hot_pay.Tax_Hotel_commision_Hotel_pay_EUR, 0) tax_hotel_comm_hotel_pay_eur, " \
    #          "       0 MISSING_Marketing_contribution,  " \
    #          "       0 MISSING_Tax_marketing_contribution,  " \
    #          "       nvl(marketing_contrib.Marketing_contribution_EUR, 0) Marketing_contribution_EUR, " \
    #          "       nvl(marketing_contrib.Tax_marketing_contribution_EUR, 0) Tax_marketing_contribution_EUR, " \
    #          "        " \
    #          "       0 MISSING_bank_expenses,  " \
    #          "       0 MISSING_Tax_Bank_expenses,  " \
    #          "       nvl(bank_exp.bank_expenses_EUR, 0) bank_expenses_EUR, " \
    #          "       nvl(bank_exp.Tax_Bank_expenses_EUR, 0) Tax_Bank_expenses_EUR, " \
    #          "       0 MISSING_Platform_Fee,  " \
    #          "       0 MISSING_Tax_Platform_fee,  " \
    #          "       nvl(platform_fee.Platform_Fee_EUR, 0) Platform_Fee_EUR, " \
    #          "       nvl(platform_fee.Tax_Platform_fee_EUR, 0) Tax_Platform_fee_EUR, " \
    #          "       0 MISSING_credit_card_fee,  " \
    #          "       0 missing_tax_credit_card_fee,   " \
    #          "       nvl(credit_card_fee.credit_card_fee_EUR, 0) credit_card_fee_EUR, " \
    #          "       nvl(credit_card_fee.Tax_credit_card_fee_EUR, 0) Tax_credit_card_fee_EUR, " \
    #          "       0 MISSING_Withholding,  " \
    #          "       0 MISSING_Tax_withholding,  " \
    #          "       nvl(withholding.Withholding_EUR, 0) Withholding_EUR, " \
    #          "       nvl(withholding.Tax_withholding_EUR, 0) Tax_withholding_EUR, " \
    #          "       0 MISSING_Local_Levy,  " \
    #          "       0 MISSING_Tax_Local_Levy,  " \
    #          "       nvl(local_levy.Local_Levy_EUR, 0) Local_Levy_EUR, " \
    #          "       nvl(local_levy.Tax_Local_Levy_EUR, 0) Tax_Local_Levy_EUR, " \
    #          "       0 MISSING_Partner_Third_commision,  " \
    #          "       0 MISSING_Tax_partner_third_commision,  " \
    #          "       nvl(partner_third_comm.Partner_Third_commision_EUR, 0) partner_third_comm_eur, " \
    #          "       nvl(partner_third_comm.Tax_partner_third_commision_EUR, 0) tax_partner_third_comm_eur, " \
    #          "       nvl(num_services.act_services, 0) NUMBER_ACTIVE_ACC_SERV " \
    #          "from " \
    #          "(select grec_Seq_rec, seq_reserva, gdiv_cod_divisa booking_currency, fec_creacion creation_date, grec_seq_rec || '-' || seq_reserva interface_id, gtto_seq_ttoo, cod_pais_cliente, nro_ad, nro_ni, " \
    #          "  gdiv_cod_divisa, fec_modifica, fec_cancelacion, fec_desde, fint_cod_interface, seq_rec_hbeds, fec_hasta, nom_general, ind_tipo_credito, gdep_cod_depart, rtre_cod_tipo_res, ind_facturable_res, " \
    #          "  ind_facturable_adm, pct_comision, pct_rappel, ind_confirma, cod_divisa_p, seq_ttoo_p, cod_suc_p, seq_agencia_p, seq_sucursal_p, seq_rec_expediente, seq_res_expediente, ind_tippag, fec_creacion  " \
    #          "  from hbgdwc.dwc_bok_t_booking " \
    #          "  where fec_creacion BETWEEN '{}'::timestamp AND '{}'::timestamp " \
    #          "    and seq_reserva>0 " \
    #          "    ) cabecera " \
    #          "inner join hbgdwc.dwc_mtd_t_ttoo t on  cabecera.gtto_seq_ttoo = t.seq_ttoo " \
    #          "inner join hbgdwc.dwc_gen_t_general_country p on (nvl(cabecera.cod_pais_cliente, t.gpai_cod_pais_mercado) = p.cod_pais) " \
    #          "left join hbgdwc.dwc_mtd_t_receptive re on cabecera.grec_Seq_rec  = re.seq_rec  " \
    #          "left join hbgdwc.dwc_mtd_t_receptive rf on cabecera.seq_rec_hbeds = rf.seq_rec  " \
    #          "left join hbgdwc.dwc_itf_t_fc_interface i on cabecera.fint_cod_interface = i.cod_interface       " \
    #          "left join (select seq_rec, seq_reserva, ind_status from hbgdwc.dwc_cry_t_cancellation_recovery_res_resale " \
    #          ") rev on (rev.seq_rec=cabecera.grec_seq_rec and rev.seq_reserva=cabecera.seq_reserva) " \
    #          "left join (select i.seq_rec, i.seq_reserva, i.aplicacion, " \
    #          "    row_number () over (partition by i.seq_rec, i.seq_reserva) as rownum " \
    #          "  from hbgdwc.dwc_bok_t_booking_information i " \
    #          "  where i.tipo_op='A' " \
    #          ") inf on (inf.seq_rec=cabecera.grec_seq_rec and inf.seq_reserva=cabecera.seq_reserva and inf.rownum=1) " \
    #          "left join (SELECT ri.seq_rec, ri.seq_reserva, MIN(ri.fec_creacion) min_fec_creacion " \
    #          "                 FROM hbgdwc.dwc_bok_t_booking_information ri " \
    #          "                WHERE ri.tipo_op = 'A' " \
    #          "                group by ri.seq_rec, ri.seq_reserva) ri on ri.seq_rec = cabecera.grec_seq_rec AND ri.seq_reserva = cabecera.seq_reserva " \
    #          "left join  " \
    #          "(select hv.grec_Seq_rec, hv.rres_seq_reserva, hv.ghor_seq_hotel, h.seq_hotel, h.izge_cod_destino, di1.ides_cod_destino, di1.nom_destino, di1.sidi_cod_idioma, " \
    #          "    row_number () over (partition by hv.grec_Seq_rec, hv.rres_seq_reserva order by grec_Seq_rec, rres_seq_reserva, ghor_seq_hotel) as rownum " \
    #          "  from (select grec_Seq_rec, rres_seq_reserva, ghor_seq_hotel from hbgdwc.dwc_bok_t_hotel_sale) hv " \
    #          "    inner join (select seq_hotel, izge_cod_destino from hbgdwc.dwc_mtd_t_hotel) h on (h.seq_hotel = hv.ghor_seq_hotel) " \
    #          "    inner join (select ides_cod_destino, nom_destino, sidi_cod_idioma from hbgdwc.dwc_itn_t_internet_destination_id) di1 on (di1.ides_cod_destino = h.izge_cod_destino AND di1.sidi_cod_idioma  = 'ENG') " \
    #          ") hotel on (hotel.grec_Seq_rec = cabecera.grec_seq_rec AND hotel.rres_seq_reserva = cabecera.seq_reserva and hotel.rownum=1) " \
    #          "left join  " \
    #          "(select o.seq_rec_other, o.seq_reserva, o.nom_contrato, o.ind_tipo_otro, o.fec_desde_other, c.seq_rec, c.nom_contrato, c.cod_destino, c.ind_tipo_otro, c.fec_desde, c.fec_hasta, di2.ides_cod_Destino, di2.sidi_cod_idioma, di2.nom_destino, " \
    #          "    row_number () over (partition by o.seq_rec_other, o.seq_reserva order by o.seq_rec_other, o.seq_reserva, o.nom_contrato) as rownum " \
    #          "  from (select seq_rec as seq_rec_other, seq_reserva, nom_contrato, ind_tipo_otro, fec_desde as fec_desde_other from hbgdwc.dwc_bok_t_other) o " \
    #          "    inner join (select seq_rec, nom_contrato, cod_destino, ind_tipo_otro, fec_desde, fec_hasta from hbgdwc.dwc_con_t_contract_other) c on (c.seq_rec = o.seq_rec_other AND c.nom_contrato = o.nom_contrato AND c.ind_tipo_otro = o.ind_tipo_otro AND o.fec_desde_other BETWEEN c.fec_desde AND c.fec_hasta) " \
    #          "    inner join (select ides_cod_Destino, sidi_cod_idioma, nom_destino from hbgdwc.dwc_itn_t_internet_destination_id) di2 on (di2.ides_cod_Destino = c.cod_destino AND di2.sidi_cod_idioma  = 'ENG') " \
    #          ") oth_con on (oth_con.seq_rec_other = cabecera.grec_seq_rec AND oth_con.seq_reserva = cabecera.seq_reserva and oth_con.rownum=1) " \
    #          "left join (SELECT ri.grec_seq_rec, ri.rres_seq_reserva, COUNT(1) act_services " \
    #          "  FROM hbgdwc.dwc_bok_t_hotel_sale ri " \
    #          "  WHERE ri.fec_cancelacion is null " \
    #          "  group by ri.grec_seq_rec, ri.rres_seq_reserva " \
    #          ") num_services on (num_services.grec_Seq_rec = cabecera.grec_seq_rec AND num_services.rres_seq_reserva = cabecera.seq_reserva) " \
    #          "left join hbgdwc.svd_david_ttv ttv on ttv.grec_seq_rec||'-'||ttv.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_tax_ttv tax_ttv on tax_ttv.grec_seq_rec||'-'||tax_ttv.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_client_com cli_comm on cli_comm.grec_seq_rec||'-'||cli_comm.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_tax_client_com tax_cli_comm on tax_cli_comm.grec_seq_rec||'-'||tax_cli_comm.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_client_rap cli_rappel on cli_rappel.grec_seq_rec||'-'||cli_rappel.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_tax_client_rap tax_cli_rappel on tax_cli_rappel.grec_seq_rec||'-'||tax_cli_rappel.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_cost booking_cost on booking_cost.grec_seq_rec||'-'||booking_cost.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_tax_cost tax_cost on tax_cost.grec_seq_rec||'-'||tax_cost.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_GSA_comm GSA_comm on GSA_comm.seq_rec||'-'||GSA_comm.seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_age_comm_hot_pay age_com_hot_pay on age_com_hot_pay.grec_seq_rec||'-'||age_com_hot_pay.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_fix_over_hot_pay fix_over_hot_pay on fix_over_hot_pay.grec_seq_rec||'-'||fix_over_hot_pay.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_var_over_hot_pay var_over_hot_pay on var_over_hot_pay.grec_seq_rec||'-'||var_over_hot_pay.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_hot_comm_hot_pay hot_comm_hot_pay on hot_comm_hot_pay.grec_seq_rec||'-'||hot_comm_hot_pay.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_marketing_contrib marketing_contrib on marketing_contrib.grec_seq_rec||'-'||marketing_contrib.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_bank_exp bank_exp on bank_exp.grec_seq_rec||'-'||bank_exp.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_platform_fee platform_fee on platform_fee.grec_seq_rec||'-'||platform_fee.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_credit_card_fee credit_card_fee on credit_card_fee.grec_seq_rec||'-'||credit_card_fee.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_withholding withholding on withholding.grec_seq_rec||'-'||withholding.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_local_levy local_levy on local_levy.grec_seq_rec||'-'||local_levy.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_partner_third_comm partner_third_comm on partner_third_comm.grec_seq_rec||'-'||partner_third_comm.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.david_tasas_cambio_flc tip on trunc(cabecera.creation_date)=tip.date and cabecera.booking_currency=tip.currency ".format(
    #     sys.argv[1], sys.argv[2])

    cursor.execute(select)

    results = cursor.fetchall()

    headers = ["grec_seq_rec", "seq_reserva", "gdiv_cod_divisa", "fec_creacion", "semp_cod_emp_rf",
               "interface_id", "operative_company", "operative_office", "operative_office_desc", "operative_incoming",
               "booking_id", "interface", "invoicing_company", "invoicing_office", "invoicing_incoming",
               "creation_date", "creation_ts", "first_booking_ts", "modification_date", "modification_ts",
               "cancellation_date", "cancellation_ts", "cancelled_booking", "status_date", "booking_service_from",
               "booking_service_to", "client_code", "customer_name", "source_market", "source_market_iso", "holder",
               "credit_type", "num_adults", "num_childrens", "department_code", "booking_type", "invoicing_booking",
               "invoicing_admin", "client_commision_esp", "client_override_esp", "confirmed_booking", "Partner_booking",
               "Partner_booking_currency", "Partner_code", "Partner_brand", "Partner_agency_code",
               "Partner_agency_brand", "Booking_file_incoming", "booking_file_number", "Accomodation_model",
               "Destination_code", "booking_currency", "TTV_booking_currency", "TTV_EUR_currency", "tax_ttv",
               "tax_ttv_eur", "tax_ttv_toms", "Tax_TTV_EUR_TOMS", "MISSING_CANCO_Tax_Sales_Transfer_pricing",
               "MISSING_CANCO_Tax_Sales_Transfer_pricing_EUR", "MISSING_CANCO_Transfer_pricing",
               "missing_canco_tax_transfer_pricing_eur", "MISSING_CANCO_Tax_Cost_Transfer_pricing",
               "MISSING_CANCO_Tax_Cost_Transfer_pricing_EUR", "Client_Commision", "Client_EUR_Commision",
               "Tax_Client_commision", "Tax_Client_EUR_commision", "client_rappel", "Client_EUR_rappel",
               "tax_client_rappel", "tax_Client_EUR_rappel", "missing_cost_booking_currency", "cost_eur_currency",
               "MISSING_tax_cost", "tax_cost_EUR", "MISSING_tax_cost_TOMS", "tax_cost_EUR_TOMS", "Application",
               "canrec_status", "GSA_Commision", "GSA_EUR_Commision", "Tax_GSA_Commision", "Tax_GSA_EUR_Commision",
               "MISSING_Agency_commision_hotel_payment", "MISSING_Tax_Agency_commision_hotel_pay",
               "agency_comm_hotel_pay_eur", "tax_agency_comm_hotel_pay_eur", "MISSING_Fix_override_hotel_payment",
               "MISSING_Tax_Fix_override_hotel_pay", "fix_override_hotel_pay_eur", "tax_fix_overr_hotel_pay_eur",
               "MISSING_Var_override_hotel_payment", "MISSING_Tax_Var_override_hotel_pay",
               "var_override_hotel_pay_eur", "Tax_Var_override_hotel_pay_EUR", "MISSING_Hotel_commision_hotel_payment",
               "MISSING_Tax_Hotel_commision_Hotel_pay", "hotel_commision_hotel_pay_eur", "tax_hotel_comm_hotel_pay_eur",
               "MISSING_Marketing_contribution", "MISSING_Tax_marketing_contribution", "Marketing_contribution_EUR",
               "Tax_marketing_contribution_EUR", "MISSING_bank_expenses", "MISSING_Tax_Bank_expenses",
               "bank_expenses_EUR", "Tax_Bank_expenses_EUR", "MISSING_Platform_Fee", "MISSING_Tax_Platform_fee",
               "Platform_Fee_EUR", "Tax_Platform_fee_EUR", "MISSING_credit_card_fee", "missing_tax_credit_card_fee",
               "credit_card_fee_EUR", "Tax_credit_card_fee_EUR", "MISSING_Withholding", "MISSING_Tax_withholding",
               "Withholding_EUR", "Tax_withholding_EUR", "MISSING_Local_Levy", "MISSING_Tax_Local_Levy",
               "Local_Levy_EUR", "Tax_Local_Levy_EUR", "MISSING_Partner_Third_commision",
               "MISSING_Tax_partner_third_commision", "partner_third_comm_eur", "tax_partner_third_comm_eur",
               "NUMBER_ACTIVE_ACC_SERV"]

    dataframe = manager.session.createDataFrame(results, headers)

    return dataframe


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


def direct(manager):
    connection = psycopg2.connect(dbname=DATABASE, host=HOST,
                                  port=PORT, user=USER, password=PASS)

    cursor = connection.cursor()

    # hotel
    select = "select hv.grec_Seq_rec, hv.rres_seq_reserva, hv.ghor_seq_hotel, h.seq_hotel, h.izge_cod_destino, " \
             "       di1.ides_cod_destino, di1.nom_destino, di1.sidi_cod_idioma, row_number () " \
             "       over (partition by hv.grec_Seq_rec, hv.rres_seq_reserva " \
             "       order by grec_Seq_rec, rres_seq_reserva, ghor_seq_hotel) as rownum " \
             "  from (select grec_Seq_rec, rres_seq_reserva, ghor_seq_hotel " \
             "          from hbgdwc.dwc_bok_t_hotel_sale) hv " \
             "  inner join (select seq_hotel, izge_cod_destino " \
             "                from hbgdwc.dwc_mtd_t_hotel) h on (h.seq_hotel = hv.ghor_seq_hotel) " \
             "  inner join (select ides_cod_destino, nom_destino, sidi_cod_idioma " \
             "                from hbgdwc.dwc_itn_t_internet_destination_id) di1 on " \
             "                (di1.ides_cod_destino = h.izge_cod_destino AND di1.sidi_cod_idioma  = 'ENG')"

    # cursor.execute(select)
    #
    # results = cursor.fetchall()
    #
    # headers = ["grec_seq_rec", "rres_seq_reserva", "ghor_seq_hotel", "seq_hotel", "izge_cod_destino",
    #            "ides_cod_destino", "nom_destino", "sidi_cod_idioma", "rownum"]
    #
    # df_hotel = manager.session.createDataFrame(results, headers)

    df_hv = manager.get_dataframe("(select grec_Seq_rec, rres_seq_reserva, ghor_seq_hotel "
                                  "   from hbgdwc.dwc_bok_t_hotel_sale) "
                                  "AS dwc_bok_t_hotel_sale")
    df_h = manager.get_dataframe("(select seq_hotel, izge_cod_destino "
                                 "   from hbgdwc.dwc_mtd_t_hotel) "
                                 "AS dwc_mtd_t_hotel")
    df_di1 = manager.get_dataframe("(select ides_cod_destino, nom_destino, sidi_cod_idioma " \
                                   "   from hbgdwc.dwc_itn_t_internet_destination_id) "
                                   "AS dwc_itn_t_internet_destination_id")

    df_hotel = df_hv.join(df_h, df_h.seq_hotel == df_hv.ghor_seq_hotel)\
        .join(df_di1, [df_di1.ides_cod_destino == df_h.izge_cod_destino,
                       df_di1.sidi_cod_idioma == 'ENG'])



    df_hotel = df_hotel.select('*', func.row_number().over(Window.partitionBy("grec_Seq_rec", "rres_seq_reserva")
                                                         .orderBy("grec_Seq_rec", "rres_seq_reserva", "ghor_seq_hotel"))
                               .alias("rownum"))

    df_hotel.show()


    # oth_con
    select = "select o.seq_rec_other, o.seq_reserva, o.nom_contrato, o.ind_tipo_otro, o.fec_desde_other, c.seq_rec, " \
             "       c.nom_contrato, c.cod_destino, c.ind_tipo_otro, c.fec_desde, c.fec_hasta, di2.ides_cod_Destino, " \
             "       di2.sidi_cod_idioma, di2.nom_destino, row_number () over (partition by o.seq_rec_other, " \
             "       o.seq_reserva order by o.seq_rec_other, o.seq_reserva, o.nom_contrato) as rownum " \
             "  from (select seq_rec as seq_rec_other, seq_reserva, nom_contrato, " \
             "               ind_tipo_otro, fec_desde as fec_desde_other " \
             "          from hbgdwc.dwc_bok_t_other) o " \
             "  inner join (select seq_rec, nom_contrato, cod_destino, ind_tipo_otro, fec_desde, fec_hasta " \
             "                from hbgdwc.dwc_con_t_contract_other) c " \
             "          on (c.seq_rec = o.seq_rec_other AND c.nom_contrato = o.nom_contrato " \
             "              AND c.ind_tipo_otro = o.ind_tipo_otro " \
             "              AND o.fec_desde_other BETWEEN c.fec_desde AND c.fec_hasta)" \
             "  inner join (select ides_cod_Destino, sidi_cod_idioma, nom_destino " \
             "                from hbgdwc.dwc_itn_t_internet_destination_id) di2 " \
             "          on (di2.ides_cod_Destino = c.cod_destino AND di2.sidi_cod_idioma  = 'ENG')"

    cursor.execute(select)

    results = cursor.fetchall()

    headers = ["seq_rec_other", "seq_reserva", "nom_contrato", "ind_tipo_otro", "fec_desde_other", "seq_rec",
               "nom_contrato", "cod_destino", "ind_tipo_otro", "fec_desde", "fec_hasta", "ides_cod_Destino",
               "sidi_cod_idioma", "nom_destino", "rownum"]

    df_oth_con = manager.session.createDataFrame(results, headers)

    # reventa
    df_reventa = manager.get_dataframe("(select seq_rec, seq_reserva, ind_status "
                                       "   from hbgdwc.dwc_cry_t_cancellation_recovery_res_resale) "
                                       "AS dwc_cry_t_cancellation_recovery_res_resale")

    # information
    select = "select i.seq_rec, i.seq_reserva, i.aplicacion, " \
             "        row_number () over (partition by i.seq_rec, i.seq_reserva) as rownum " \
             "   from hbgdwc.dwc_bok_t_booking_information i where i.tipo_op='A'"

    cursor.execute(select)

    results = cursor.fetchall()

    headers = ["seq_rec", "seq_reserva", "aplicacion", "rownum"]

    df_information = manager.session.createDataFrame(results, headers)

    # num_services
    select = "SELECT ri.grec_seq_rec, ri.rres_seq_reserva, COUNT(1) act_services" \
             "  FROM hbgdwc.dwc_bok_t_hotel_sale ri" \
             " WHERE ri.fec_cancelacion is null" \
             " group by ri.grec_seq_rec, ri.rres_seq_reserva"

    cursor.execute(select)

    results = cursor.fetchall()

    headers = ["grec_seq_rec", "rres_seq_reserva", "act_services"]

    df_num_services = manager.session.createDataFrame(results, headers)

    # cabecera

    cabecera = "(select grec_Seq_rec, seq_reserva, gdiv_cod_divisa booking_currency, fec_creacion creation_date, " \
               "       grec_seq_rec || '-' || seq_reserva interface_id, gtto_seq_ttoo, cod_pais_cliente, nro_ad, " \
               "       nro_ni, gdiv_cod_divisa, fec_modifica, fec_cancelacion, fec_desde, fint_cod_interface, " \
               "       seq_rec_hbeds, fec_hasta, nom_general, ind_tipo_credito, gdep_cod_depart, rtre_cod_tipo_res, " \
               "       ind_facturable_res, ind_facturable_adm, pct_comision, pct_rappel, ind_confirma, cod_divisa_p, " \
               "       seq_ttoo_p, cod_suc_p, seq_agencia_p, seq_sucursal_p, seq_rec_expediente, seq_res_expediente, " \
               "       ind_tippag " \
               "  from hbgdwc.dwc_bok_t_booking" \
               " where fec_creacion BETWEEN '{}'::timestamp AND '{}'::timestamp" \
               "   and seq_reserva>0) AS dwc_bok_t_booking".format(sys.argv[1], sys.argv[2])

    df_cabecera = manager.get_dataframe(cabecera)

    select = "SELECT ri.seq_rec, ri.seq_reserva, MIN(ri.fec_creacion) min_fec_creacion" \
             "  FROM hbgdwc.dwc_bok_t_booking_information ri" \
             " WHERE ri.tipo_op = 'A'" \
             " group by ri.seq_rec, ri.seq_reserva"

    cursor.execute(select)

    results = cursor.fetchall()

    headers = ["seq_rec", "seq_reserva", "min_fec_creacion"]

    df_ri = manager.session.createDataFrame(results, headers)


    df_t = manager.get_dataframe("hbgdwc.dwc_mtd_t_ttoo")
    df_p = manager.get_dataframe("hbgdwc.dwc_gen_t_general_country")

    df_re = manager.get_dataframe("hbgdwc.dwc_mtd_t_receptive")
    # df_rf = manager.get_dataframe("hbgdwc.dwc_gen_t_general_country")
    df_i = manager.get_dataframe("hbgdwc.dwc_itf_t_fc_interface")
    df_ttv = manager.get_dataframe("hbgdwc.svd_david_ttv")
    df_tax_ttv = manager.get_dataframe("hbgdwc.svd_david_tax_ttv")
    df_cli_comm = manager.get_dataframe("hbgdwc.svd_david_client_com")
    df_cli_rappel = manager.get_dataframe("hbgdwc.svd_david_client_rap")
    df_tax_cli_rappel = manager.get_dataframe("hbgdwc.svd_david_tax_client_rap")
    df_booking_cost = manager.get_dataframe("hbgdwc.svd_david_cost")
    df_tax_cost = manager.get_dataframe("hbgdwc.svd_david_tax_cost")
    df_GSA_comm = manager.get_dataframe("hbgdwc.svd_david_GSA_comm")
    df_age_comm_hot_pay = manager.get_dataframe("hbgdwc.svd_david_age_comm_hot_pay")
    df_fix_over_hot_pay = manager.get_dataframe("hbgdwc.svd_david_fix_over_hot_pay")
    df_var_over_hot_pay = manager.get_dataframe("hbgdwc.svd_david_var_over_hot_pay")
    df_hot_comm_hot_pay = manager.get_dataframe("hbgdwc.svd_david_hot_comm_hot_pay")
    df_marketing_contrib = manager.get_dataframe("hbgdwc.svd_david_marketing_contrib")
    df_bank_exp = manager.get_dataframe("hbgdwc.svd_david_bank_exp")
    df_platform_fee = manager.get_dataframe("hbgdwc.svd_david_platform_fee")
    df_credit_card_fee = manager.get_dataframe("hbgdwc.svd_david_credit_card_fee")
    df_withholding = manager.get_dataframe("hbgdwc.svd_david_withholding")
    df_local_levy = manager.get_dataframe("hbgdwc.svd_david_local_levy")
    df_partner_third_comm = manager.get_dataframe("hbgdwc.svd_david_partner_third_comm")
    df_tip = manager.get_dataframe("hbgdwc.david_tasas_cambio_flc")

    # create inmemory views
    df_hotel.createOrReplaceTempView("hotel")
    df_oth_con.createOrReplaceTempView("oth_con")
    df_reventa.createOrReplaceTempView("rev")
    df_information.createOrReplaceTempView("inf")
    df_num_services.createOrReplaceTempView("num_services")
    df_cabecera.createOrReplaceTempView("cabecera")
    df_ri.createOrReplaceTempView("ri")

    df_t.createOrReplaceTempView("t")
    df_p.createOrReplaceTempView("p")

    df_re.createOrReplaceTempView("re")
    df_re.createOrReplaceTempView("rf")
    df_i.createOrReplaceTempView("i")
    df_ttv.createOrReplaceTempView("ttv")
    df_tax_ttv.createOrReplaceTempView("tax_ttv")
    df_cli_comm.createOrReplaceTempView("cli_com")
    df_cli_rappel.createOrReplaceTempView("cli_rappel")
    df_tax_cli_rappel.createOrReplaceTempView("tax_cli_rappel")
    df_booking_cost.createOrReplaceTempView("_booking_cost")
    df_tax_cost.createOrReplaceTempView("tax_cost")
    df_GSA_comm.createOrReplaceTempView("GSA_comm")
    df_age_comm_hot_pay.createOrReplaceTempView("age_comm_hot_pay")
    df_fix_over_hot_pay.createOrReplaceTempView("fix_over_hot_pay")
    df_var_over_hot_pay.createOrReplaceTempView("var_over_hot_pay")
    df_hot_comm_hot_pay.createOrReplaceTempView("hot_comm_hot_pay")
    df_marketing_contrib.createOrReplaceTempView("marketing_contrib")
    df_bank_exp.createOrReplaceTempView("bank_exp")
    df_platform_fee.createOrReplaceTempView("platform_fee")
    df_credit_card_fee.createOrReplaceTempView("credit_card_fee")
    df_withholding.createOrReplaceTempView("withholding")
    df_local_levy.createOrReplaceTempView("local_levy")
    df_partner_third_comm.createOrReplaceTempView("partner_third_comm")
    df_tip.createOrReplaceTempView("tip")

    select = "select cabecera.grec_seq_rec grec_seq_rec, " \
             "       cabecera.seq_reserva seq_reserva, " \
             "       cabecera.gdiv_cod_divisa gdiv_cod_divisa, " \
             "       cabecera.fec_creacion fec_creacion, " \
             "       rf.semp_cod_emp semp_cod_emp_rf, " \
             "       cabecera.interface_id, " \
             "       re.semp_cod_emp operative_company, " \
             "       re.sofi_cod_ofi operative_office, " \
             "       re.des_receptivo operative_office_desc, " \
             "       cabecera.grec_seq_rec operative_incoming, " \
             "       cabecera.seq_reserva booking_id, " \
             "       cabecera.fint_cod_interface interface, " \
             "       rf.semp_cod_emp invoicing_company, " \
             "       rf.sofi_cod_ofi invoicing_office, " \
             "       rf.seq_rec invoicing_incoming, " \
             "       TRUNC (cabecera.creation_date) creation_date, " \
             "       cabecera.creation_date creation_ts, " \
             "       ri.min_fec_creacion first_booking_ts, " \
             "       NVL(TO_CHAR(TRUNC (cabecera.fec_modifica),'yyyy-mm-dd'),'') modification_date, " \
             "       NVL(TO_CHAR(cabecera.fec_modifica,'yyyy-mm-dd hh24:mi:ss'),'') modification_ts, " \
             "       NVL(TO_CHAR(TRUNC (cabecera.fec_cancelacion),'yyyy-mm-dd'),'') cancellation_date, " \
             "       NVL(TO_CHAR(cabecera.fec_cancelacion,'yyyy-mm-dd hh24:mi:ss'),'') cancellation_ts, " \
             "       decode(cabecera.fec_cancelacion, null, 'N', 'S') cancelled_booking, " \
             "       TRUNC(GREATEST(cabecera.creation_date, cabecera.fec_modifica, cabecera.fec_cancelacion)) status_date, " \
             "       trunc(cabecera.fec_desde) booking_service_from, " \
             "       trunc(cabecera.fec_hasta) booking_service_to, " \
             "       t.seq_ttoo client_code, " \
             "       t.nom_corto_ttoo customer_name, " \
             "       NVL (cabecera.cod_pais_cliente, t.gpai_cod_pais_mercado) source_market, " \
             "       p.cod_iso source_market_iso, " \
             "       REPLACE(cabecera.nom_general,';','') holder, " \
             "       cabecera.ind_tipo_credito credit_type, " \
             "       cabecera.nro_ad num_adults, " \
             "       cabecera.nro_ni num_childrens, " \
             "       cabecera.gdep_cod_depart department_code, " \
             "       cabecera.rtre_cod_tipo_res booking_type, " \
             "       cabecera.ind_facturable_res invoicing_booking, " \
             "       cabecera.ind_facturable_adm invoicing_admin, " \
             "       NVL(cabecera.pct_comision,0) Client_commision_esp, " \
             "       NVL(cabecera.pct_rappel,0) client_override_esp, " \
             "       cabecera.ind_confirma confirmed_booking, " \
             "       decode (i.partner_ttoo, null, 'N', 'S') Partner_booking, " \
             "       NVL(cabecera.cod_divisa_p,'') Partner_booking_currency, " \
             "       NVL(cabecera.seq_ttoo_p,0) Partner_code, " \
             "       NVL(cabecera.cod_suc_p,0) Partner_brand, " \
             "       NVl(cabecera.seq_agencia_p,0) Partner_agency_code, " \
             "       NVL(cabecera.seq_sucursal_p,0) Partner_agency_brand, " \
             "       NVL(cabecera.seq_rec_expediente,0) Booking_file_incoming, " \
             "       NVL(cabecera.seq_res_expediente,0) booking_file_number, " \
             "       decode (cabecera.ind_tippag, null, 'Merchant', 'Pago en hotel') Accomodation_model, " \
             "       NVL ( hotel.izge_cod_destino || '-' || hotel.nom_destino, oth_con.cod_destino || '-' ||oth_con.nom_destino, 'NO_DESTINATION_CODE'  ) Destination_code, " \
             "       cabecera.gdiv_cod_divisa booking_currency, " \
             "       nvl(ttv.ttv, 0) TTV_booking_currency, " \
             "       nvl(ttv.ttv, 0)*nvl(tip.rate, 0) TTV_EUR_currency, " \
             "       nvl(tax_ttv.tax_ttv, 0) tax_ttv, " \
             "       nvl(tax_ttv.tax_ttv, 0)*nvl(tip.rate, 0) tax_ttv_eur, " \
             "       nvl(tax_ttv.tax_ttv_toms, 0) tax_ttv_toms, " \
             "       nvl(tax_ttv.tax_ttv_toms, 0)*nvl(tip.rate, 0) Tax_TTV_EUR_TOMS, " \
             "       0 MISSING_CANCO_Tax_Sales_Transfer_pricing, " \
             "       0 MISSING_CANCO_Tax_Sales_Transfer_pricing_EUR, " \
             "       0 MISSING_CANCO_Transfer_pricing, " \
             "       0 missing_canco_tax_transfer_pricing_eur, " \
             "       0 MISSING_CANCO_Tax_Cost_Transfer_pricing, " \
             "       0 MISSING_CANCO_Tax_Cost_Transfer_pricing_EUR, " \
             "       nvl(cli_comm.cli_comm, 0) Client_Commision, " \
             "       nvl(cli_comm.cli_comm, 0)*nvl(tip.rate, 0) Client_EUR_Commision, " \
             "       nvl(tax_cli_comm.Tax_Client_Com, 0) Tax_Client_commision, " \
             "       nvl(tax_cli_comm.Tax_Client_Com, 0)*nvl(tip.rate, 0) Tax_Client_EUR_commision, " \
             "       nvl(cli_rappel.cli_rappel, 0) client_rappel, " \
             "       nvl(cli_rappel.cli_rappel, 0)*nvl(tip.rate, 0) Client_EUR_rappel, " \
             "       nvl(tax_cli_rappel.tax_cli_rappel, 0) tax_client_rappel, " \
             "       nvl(tax_cli_rappel.tax_cli_rappel, 0)*nvl(tip.rate, 0) tax_Client_EUR_rappel, " \
             "       0 as missing_cost_booking_currency,   " \
             "       nvl(booking_cost.booking_cost, 0) cost_eur_currency, " \
             "       0 as MISSING_tax_cost, " \
             "       nvl(tax_cost.tax_cost, 0) tax_cost_EUR, " \
             "       0 as MISSING_tax_cost_TOMS, " \
             "       nvl(tax_cost.tax_cost_TOMS, 0) tax_cost_EUR_TOMS, " \
             "       inf.aplicacion Application, " \
             "       decode( " \
             "              decode(rev.ind_status, " \
             "                      'RR', 'R', " \
             "                      'CN', 'RL', " \
             "                      'RL', decode(cabecera.fec_cancelacion, null, 'RS', 'C'), " \
             "                      rev.ind_status), " \
             "              'RS', 'Reventa', " \
             "              'C', 'Cancelada', " \
             "              'RL', 'Liberada', " \
             "              'R', 'Reventa') canrec_status, " \
             "       nvl(GSA_comm.GSA_Comm, 0) GSA_Commision, " \
             "       nvl(GSA_comm.GSA_Comm, 0)*nvl(tip.rate, 0) GSA_EUR_Commision, " \
             "       nvl(GSA_comm.Tax_GSA_Comm, 0) Tax_GSA_Commision, " \
             "       nvl(GSA_comm.Tax_GSA_Comm, 0)*nvl(tip.rate, 0) Tax_GSA_EUR_Commision, " \
             "       0 MISSING_Agency_commision_hotel_payment,  " \
             "       0 MISSING_Tax_Agency_commision_hotel_pay,  " \
             "       nvl(age_com_hot_pay.Agency_commision_hotel_payment_EUR, 0) agency_comm_hotel_pay_eur, " \
             "       nvl(age_com_hot_pay.Tax_Agency_commision_hotel_pay_EUR, 0) tax_agency_comm_hotel_pay_eur, " \
             "       0 MISSING_Fix_override_hotel_payment,  " \
             "       0 MISSING_Tax_Fix_override_hotel_pay,  " \
             "       nvl(fix_over_hot_pay.Fix_override_hotel_payment_EUR, 0) fix_override_hotel_pay_eur, " \
             "       nvl(fix_over_hot_pay.Tax_Fix_override_hotel_pay_EUR, 0) tax_fix_overr_hotel_pay_eur, " \
             "       0 MISSING_Var_override_hotel_payment,  " \
             "       0 MISSING_Tax_Var_override_hotel_pay,  " \
             "       nvl(var_over_hot_pay.Var_override_hotel_payment_EUR, 0) var_override_hotel_pay_eur, " \
             "       nvl(var_over_hot_pay.Tax_Var_override_hotel_pay_EUR, 0) Tax_Var_override_hotel_pay_EUR, " \
             "       0 MISSING_Hotel_commision_hotel_payment,  " \
             "       0 MISSING_Tax_Hotel_commision_Hotel_pay,  " \
             "       nvl(hot_comm_hot_pay.Hotel_commision_hotel_payment_EUR, 0) hotel_commision_hotel_pay_eur, " \
             "       nvl(hot_comm_hot_pay.Tax_Hotel_commision_Hotel_pay_EUR, 0) tax_hotel_comm_hotel_pay_eur, " \
             "       0 MISSING_Marketing_contribution,  " \
             "       0 MISSING_Tax_marketing_contribution,  " \
             "       nvl(marketing_contrib.Marketing_contribution_EUR, 0) Marketing_contribution_EUR, " \
             "       nvl(marketing_contrib.Tax_marketing_contribution_EUR, 0) Tax_marketing_contribution_EUR, " \
             "        " \
             "       0 MISSING_bank_expenses,  " \
             "       0 MISSING_Tax_Bank_expenses,  " \
             "       nvl(bank_exp.bank_expenses_EUR, 0) bank_expenses_EUR, " \
             "       nvl(bank_exp.Tax_Bank_expenses_EUR, 0) Tax_Bank_expenses_EUR, " \
             "       0 MISSING_Platform_Fee,  " \
             "       0 MISSING_Tax_Platform_fee,  " \
             "       nvl(platform_fee.Platform_Fee_EUR, 0) Platform_Fee_EUR, " \
             "       nvl(platform_fee.Tax_Platform_fee_EUR, 0) Tax_Platform_fee_EUR, " \
             "       0 MISSING_credit_card_fee,  " \
             "       0 missing_tax_credit_card_fee,   " \
             "       nvl(credit_card_fee.credit_card_fee_EUR, 0) credit_card_fee_EUR, " \
             "       nvl(credit_card_fee.Tax_credit_card_fee_EUR, 0) Tax_credit_card_fee_EUR, " \
             "       0 MISSING_Withholding,  " \
             "       0 MISSING_Tax_withholding,  " \
             "       nvl(withholding.Withholding_EUR, 0) Withholding_EUR, " \
             "       nvl(withholding.Tax_withholding_EUR, 0) Tax_withholding_EUR, " \
             "       0 MISSING_Local_Levy,  " \
             "       0 MISSING_Tax_Local_Levy,  " \
             "       nvl(local_levy.Local_Levy_EUR, 0) Local_Levy_EUR, " \
             "       nvl(local_levy.Tax_Local_Levy_EUR, 0) Tax_Local_Levy_EUR, " \
             "       0 MISSING_Partner_Third_commision,  " \
             "       0 MISSING_Tax_partner_third_commision,  " \
             "       nvl(partner_third_comm.Partner_Third_commision_EUR, 0) partner_third_comm_eur, " \
             "       nvl(partner_third_comm.Tax_partner_third_commision_EUR, 0) tax_partner_third_comm_eur, " \
             "       nvl(num_services.act_services, 0) NUMBER_ACTIVE_ACC_SERV " \
             "  from cabecera " \
             " inner join t on  cabecera.gtto_seq_ttoo = t.seq_ttoo " \
             " inner join p on (nvl(cabecera.cod_pais_cliente, t.gpai_cod_pais_mercado) = p.cod_pais) " \
             " left join re on cabecera.grec_Seq_rec  = re.seq_rec  " \
             " left join rf on cabecera.seq_rec_hbeds = rf.seq_rec  " \
             " left join i on cabecera.fint_cod_interface = i.cod_interface       " \
             " left join rev on (rev.seq_rec=cabecera.grec_seq_rec and rev.seq_reserva=cabecera.seq_reserva) " \
             " left join inf on (inf.seq_rec=cabecera.grec_seq_rec and inf.seq_reserva=cabecera.seq_reserva and inf.rownum=1) " \
             " left join ri on ri.seq_rec = cabecera.grec_seq_rec AND ri.seq_reserva = cabecera.seq_reserva " \
             " left join hotel on (hotel.grec_Seq_rec = cabecera.grec_seq_rec AND hotel.rres_seq_reserva = cabecera.seq_reserva and hotel.rownum=1)" \
             " left join oth_con on (oth_con.seq_rec_other = cabecera.grec_seq_rec AND oth_con.seq_reserva = cabecera.seq_reserva and oth_con.rownum=1)" \
             " left join num_services on (num_services.grec_Seq_rec = cabecera.grec_seq_rec AND num_services.rres_seq_reserva = cabecera.seq_reserva)" \
             " left join ttv on ttv.grec_seq_rec||'-'||ttv.rres_seq_reserva=cabecera.interface_id" \
             " left join tax_ttv on tax_ttv.grec_seq_rec||'-'||tax_ttv.rres_seq_reserva=cabecera.interface_id" \
             " left join cli_comm on cli_comm.grec_seq_rec||'-'||cli_comm.rres_seq_reserva=cabecera.interface_id" \
             " left join tax_cli_comm on tax_cli_comm.grec_seq_rec||'-'||tax_cli_comm.rres_seq_reserva=cabecera.interface_id" \
             " left join cli_rappel on cli_rappel.grec_seq_rec||'-'||cli_rappel.rres_seq_reserva=cabecera.interface_id" \
             " left join tax_cli_rappel on tax_cli_rappel.grec_seq_rec||'-'||tax_cli_rappel.rres_seq_reserva=cabecera.interface_id" \
             " left join booking_cost on booking_cost.grec_seq_rec||'-'||booking_cost.rres_seq_reserva=cabecera.interface_id" \
             " left join tax_cost on tax_cost.grec_seq_rec||'-'||tax_cost.rres_seq_reserva=cabecera.interface_id" \
             " left join GSA_comm on GSA_comm.seq_rec||'-'||GSA_comm.seq_reserva=cabecera.interface_id" \
             " left join age_com_hot_pay on age_com_hot_pay.grec_seq_rec||'-'||age_com_hot_pay.rres_seq_reserva=cabecera.interface_id" \
             " left join fix_over_hot_pay on fix_over_hot_pay.grec_seq_rec||'-'||fix_over_hot_pay.rres_seq_reserva=cabecera.interface_id" \
             " left join var_over_hot_pay on var_over_hot_pay.grec_seq_rec||'-'||var_over_hot_pay.rres_seq_reserva=cabecera.interface_id" \
             " left join hot_comm_hot_pay on hot_comm_hot_pay.grec_seq_rec||'-'||hot_comm_hot_pay.rres_seq_reserva=cabecera.interface_id" \
             " left join marketing_contrib on marketing_contrib.grec_seq_rec||'-'||marketing_contrib.rres_seq_reserva=cabecera.interface_id" \
             " left join bank_exp on bank_exp.grec_seq_rec||'-'||bank_exp.rres_seq_reserva=cabecera.interface_id" \
             " left join platform_fee on platform_fee.grec_seq_rec||'-'||platform_fee.rres_seq_reserva=cabecera.interface_id" \
             " left join credit_card_fee on credit_card_fee.grec_seq_rec||'-'||credit_card_fee.rres_seq_reserva=cabecera.interface_id" \
             " left join withholding on withholding.grec_seq_rec||'-'||withholding.rres_seq_reserva=cabecera.interface_id" \
             " left join local_levy on local_levy.grec_seq_rec||'-'||local_levy.rres_seq_reserva=cabecera.interface_id" \
             " left join partner_third_comm on partner_third_comm.grec_seq_rec||'-'||partner_third_comm.rres_seq_reserva=cabecera.interface_id" \
             " left join tip on trunc(cabecera.creation_date)=tip.date and cabecera.booking_currency=tip.currency"


    df_direct_fields = manager.session.sql(select)

    return df_direct_fields


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
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
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
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
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

    df_impuesto_canal = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(
        df_extra)

    df_impuesto_canal = df_impuesto_canal.groupBy("seq_rec", "seq_reserva") \
        .agg({'impuesto_canal': 'sum'}).withColumnRenamed("SUM(impuesto_canal)", "Tax_Sales_Transfer_pricing")

    df_fields = df_fields.join(df_impuesto_canal, [df_fields.grec_seq_rec == df_impuesto_canal.seq_rec,
                                                   df_fields.seq_reserva == df_impuesto_canal.seq_reserva],
                               'left_outer').drop(df_impuesto_canal.seq_rec).drop(df_impuesto_canal.seq_reserva)

    df_fields = df_fields.na.fill({"Tax_Sales_Transfer_pricing": 0})

    df_fields = df_fields.withColumn("Tax_Sales_Transfer_pricing",
                                     udf_round_ccy(df_fields.Tax_Sales_Transfer_pricing,
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
                                         dataframe.imp_venta * (
                                             1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100)))) - (
                                             dataframe.imp_venta - dataframe.imp_margen_canal) * (
                                             1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))
                                     ))

    dataframe = dataframe.withColumn("impuesto_canco2",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
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
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
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
                                         dataframe.imp_venta * (
                                             1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100)))) - (
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
                df_res.rext_ord_extra, df_res.sdiv_cod_divisa, df_res.fec_desde, df_res.fec_hasta,
                df_res.nro_unidades,
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

    df_impuesto_canco = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(
        df_extra)

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
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
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
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
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

    df_impuesto_canco = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(
        df_extra)

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
                                               dataframe.imp_margen_canal * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
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
                                               dataframe.imp_margen_canal * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
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

    df_impuesto_canal = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(
        df_extra)

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


# def sub_tax_sales_transfer_pricing_eur(manager, df_fields, seq_recs, seq_reservas):
#     df_fields = df_fields.withColumn('Tax_Sales_Transfer_pricing_EUR', udf_currency_exchange(df_fields.semp_cod_emp_rf,
#                                                                              func.lit(EXCHANGE_RES),
#                                                                              df_fields.fec_creacion,
#                                                                              df_fields.gdiv_cod_divisa,
#                                                                              df_fields.Tax_Sales_Transfer_pricing,
#                                                                              func.lit(EUR)))
#
#     df_fields = df_fields.na.fill({'Tax_Sales_Transfer_pricing_EUR':0})
#
#     return df_fields


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
                                               dataframe.imp_margen_canal * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                  / 100)))))
                                     .otherwise(dataframe.imp_venta * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_venta - dataframe.imp_margen_canal) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.withColumn("amount2",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
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
                                               dataframe.imp_margen_canal * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                  / 100)))))
                                     .otherwise(dataframe.imp_venta * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_venta - dataframe.imp_margen_canal) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.withColumn("amount2",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
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

    df_impuesto_canco = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(
        df_extra)

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
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
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
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
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

    df_impuesto_canco = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(
        df_extra)

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


def remove_fields(dataframe):
    dataframe = dataframe.drop("MISSING_CANCO_Tax_Sales_Transfer_pricing") \
        .drop("MISSING_CANCO_Tax_Sales_Transfer_pricing_EUR") \
        .drop("MISSING_CANCO_Tax_Sales_Transfer_pricing_EUR") \
        .drop("MISSING_CANCO_Transfer_pricing") \
        .drop("missing_canco_tax_transfer_pricing_eur") \
        .drop("MISSING_CANCO_Tax_Cost_Transfer_pricing") \
        .drop("MISSING_CANCO_Tax_Cost_Transfer_pricing_EUR") \
        .drop("gdiv_cod_divisa") \
        .drop("fec_creacion") \
        .drop("semp_cod_emp_rf")

    return dataframe
