tables = {"dwc_bok_t_booking": "(SELECT grec_seq_rec || '-' || seq_reserva interface_id,"
                               "       grec_seq_rec,"
                               "       seq_reserva,"
                               "       fint_cod_interface,"
                               "       fec_creacion,"
                               "       fec_modifica,"
                               "       fec_cancelacion,"
                               "       fec_desde,"
                               "       fec_hasta,"
                               "       cod_pais_cliente,"
                               "       nom_general,"
                               "       nro_ad,"
                               "       nro_ni,"
                               "       gdep_cod_depart,"
                               "       rtre_cod_tipo_res,"
                               "       ind_facturable_res,"
                               "       ind_facturable_adm,"
                               "       pct_comision,"
                               "       pct_rappel,"
                               "       ind_confirma,"
                               "       cod_divisa_p,"
                               "       seq_ttoo_p,"
                               "       cod_suc_p,"
                               "       seq_agencia_p,"
                               "       seq_sucursal_p,"
                               "       seq_rec_expediente,"
                               "       seq_res_expediente,"
                               "       gdiv_cod_divisa,"
                               "       gtto_seq_ttoo,"
                               "       ind_tippag,"
                               "       seq_rec_hbeds"
                               "  FROM hbgdwc.dwc_bok_t_booking"
                               " WHERE fec_modifica BETWEEN '{}' AND '{}' "
                               "   AND fec_creacion BETWEEN '{}' AND '{}') as dwc_bok_t_booking",
          "dwc_mtd_t_ttoo": "(SELECT seq_ttoo,"
                            "        nom_corto_ttoo,"
                            "        gpai_cod_pais_mercado"
                            "   FROM hbgdwc.dwc_mtd_t_ttoo) as dwc_mtd_t_ttoo",
          "dwc_mtd_t_receptive": "(SELECT re.semp_cod_emp semp_cod_emp_re,"
                                 "       re.sofi_cod_ofi sofi_cod_ofi_re, "
                                 "       des_receptivo,"
                                 "       re.seq_rec seq_rec_re"
                                 "  FROM hbgdwc.dwc_mtd_t_receptive re) as dwc_mtd_t_receptive",

          # select literal
          # "dwc_mtd_t_receptive": "(SELECT re.semp_cod_emp ,"
          #                        "       re.sofi_cod_ofi , "
          #                        "       des_receptivo,"
          #                        "       re.seq_rec "
          #                        "  FROM hbgdwc.dwc_mtd_t_receptive re) as dwc_mtd_t_receptive",

          "dwc_mtd_t_receptive_f": "(SELECT rf.semp_cod_emp semp_cod_emp_rf,"
                                   "       rf.sofi_cod_ofi sofi_cod_ofi_rf,"
                                   "       rf.seq_rec seq_rec_rf"
                                   "  FROM hbgdwc.dwc_mtd_t_receptive rf) as dwc_mtd_t_receptive_f",
          "dwc_itf_t_fc_interface": "(SELECT cod_interface, "
                                    "        partner_ttoo, "
                                    "        ind_fec_cam_div"
                                    "  FROM hbgdwc.dwc_itf_t_fc_interface i) as dwc_itf_t_fc_interface",
          "dwc_gen_t_general_country": "(SELECT cod_iso,"
                                       "       cod_pais "
                                       "  FROM hbgdwc.dwc_gen_t_general_country) as dwc_gen_t_general_country",

          # SUBSELECTS

          "dwc_bok_t_booking_information": "(select fec_creacion fec_creacion_info,"
                                           "        seq_rec,"
                                           "        seq_reserva"
                                           "   FROM hbgdwc.dwc_bok_t_booking_information"
                                           "  where tipo_op = 'A') as dwc_bok_t_booking_information",

          # select literal
          # "dwc_bok_t_booking_information": "(select fec_creacion,"
          #                                  "        seq_rec,"
          #                                  "        tipo_op,"
          #                                  "        seq_reserva"
          #                                  "   FROM hbgdwc.dwc_bok_t_booking_information"
          #                                  " ) as dwc_bok_t_booking_information",


          ###
          "dwc_mtd_t_hotel": "(select izge_cod_destino, seq_hotel "
                             "   from hbgdwc.dwc_mtd_t_hotel) as dwc_mtd_t_hotel",
          "dwc_bok_t_hotel_sale": "(select grec_Seq_rec, rres_seq_reserva, ghor_seq_hotel "
                                  "   from hbgdwc.dwc_bok_t_hotel_sale) as dwc_bok_t_hotel_sale",
          "dwc_itn_t_internet_destination_id": "(select nom_destino, ides_cod_destino, sidi_cod_idioma "
                                               "   from hbgdwc.dwc_itn_t_internet_destination_id) "
                                               "     as dwc_itn_t_internet_destination_id",

          # select literal
          # "dwc_mtd_t_hotel": "(select * "
          #                    "   from hbgdwc.dwc_mtd_t_hotel) as dwc_mtd_t_hotel",
          # "dwc_bok_t_hotel_sale": "(select * "
          #                         "   from hbgdwc.dwc_bok_t_hotel_sale) as dwc_bok_t_hotel_sale",
          # "dwc_itn_t_internet_destination_id": "(select * "
          #                                      "   from hbgdwc.dwc_itn_t_internet_destination_id) "
          #                                      "     as dwc_itn_t_internet_destination_id",


          ###
          "dwc_bok_t_other": "(select seq_rec, seq_reserva, nom_contrato, ind_tipo_otro, fec_desde "
                             "   from hbgdwc.dwc_bok_t_other) as dwc_bok_t_other",
          "dwc_con_t_contract_other": "(select cod_destino, seq_rec, nom_contrato, ind_tipo_otro, fec_desde, fec_hasta "
                                      "   from hbgdwc.dwc_con_t_contract_other) as dwc_con_t_contract_other",
          ###
# select literal
#           "dwc_bok_t_sale": "(select fec_desde, fec_hasta, nro_unidades, nro_pax, "
#                             "        ind_tipo_unid, ind_p_s, imp_unitario, "
#                             "        grec_seq_rec, rres_seq_reserva, ind_tipo_registro, Ind_Facturable, "
#                             "        ind_contra_apunte ,"
#                             "        ind_trfprc, imp_impuesto, ind_tipo_regimen, rvp_ind_tipo_imp, "
#                             "        rvp_cod_impuesto, rvp_cod_esquema, rvp_cod_clasif, rvp_cod_empresa "
#                             "   from hbgdwc.dwc_bok_t_sale) as dwc_bok_t_sale",
          "dwc_bok_t_sale": "(select fec_desde fec_desde_s, fec_hasta fec_hasta_s, nro_unidades, nro_pax, "
                            "        ind_tipo_unid, ind_p_s, imp_unitario, "
                            "        grec_seq_rec grec_seq_rec_s, rres_seq_reserva, ind_tipo_registro, Ind_Facturable, "
                            "        ind_contra_apunte ,"
                            "        ind_trfprc, imp_impuesto, ind_tipo_regimen, rvp_ind_tipo_imp, "
                            "        rvp_cod_impuesto, rvp_cod_esquema, rvp_cod_clasif, rvp_cod_empresa "
                            "   from hbgdwc.dwc_bok_t_sale) as dwc_bok_t_sale",
          # "dwc_bok_t_sale": "(select fec_desde fec_desde_s, fec_hasta fec_hasta_s, nro_unidades, nro_pax, "
          #                   "        ind_tipo_unid, ind_p_s, imp_unitario, "
          #                   "        grec_seq_rec, rres_seq_reserva, ind_tipo_registro, Ind_Facturable, "
          #                   "        ind_contra_apunte, imp_impuesto, ind_tipo_regimen, Rvp_Ind_Tipo_Imp, "
          #                   "        Rvp_Cod_Impuesto, Rvp_Cod_Esquema, Rvp_Cod_Clasif, Rvp_Cod_Empresa "
          #                   "   from hbgdwc.dwc_bok_t_sale) as dwc_bok_t_sale",
          "dwc_oth_v_re_v_impuesto_sap": "(select pct_impuesto, ind_tipo_imp, cod_impuesto, "
                                         "        cod_esquema, cod_clasif, cod_emp_atlas"
                                         "        from hbgdwc.dwc_oth_v_re_v_impuesto_sap) "
                                         "     as dwc_oth_v_re_v_impuesto_sap",
          "dwc_oth_v_re_v_impuesto_sap_vta": "(select pct_impuesto pct_impuesto_vta, ind_tipo_imp ind_tipo_imp_vta, "
                                             "        cod_impuesto cod_impuesto_vta, cod_esquema cod_esquema_vta, "
                                             "        cod_clasif cod_clasif_vta, cod_emp_atlas cod_emp_atlas_vta"
                                             "        from hbgdwc.dwc_oth_v_re_v_impuesto_sap) "
                                             "     as dwc_oth_v_re_v_impuesto_sap_vta",
          "dwc_oth_v_re_v_impuesto_sap_cpa": "(select pct_impuesto pct_impuesto_cpa, ind_tipo_imp ind_tipo_imp_cpa, "
                                             "        cod_impuesto cod_impuesto_cpa,  cod_esquema cod_esquema_cpa, "
                                             "        cod_clasif cod_clasif_cpa, cod_emp_atlas cod_emp_atlas_cpa"
                                             "        from hbgdwc.dwc_oth_v_re_v_impuesto_sap) "
                                             "     as dwc_oth_v_re_v_impuesto_sap_cpa",
          "dwc_bok_t_canco_hotel": "(select seq_rec, seq_reserva, ind_tipo_regimen_fac, imp_margen_canal, "
                                   "        imp_margen_canco, imp_venta, ind_tipo_imp_vta_fac, cod_impuesto_vta_fac, "
                                   "        cod_clasif_vta_fac, cod_esquema_vta_fac, cod_empresa_vta_fac,"
                                   "        ind_tipo_regimen_con, imp_coste "
                                   "   from hbgdwc.dwc_bok_t_canco_hotel) as dwc_bok_t_canco_hotel",
          "dwc_bok_t_canco_hotel_circuit": "(select seq_rec, seq_reserva, ind_tipo_regimen_fac, imp_margen_canal, "
                                           "        imp_margen_canco, imp_venta, ind_tipo_imp_vta_fac, "
                                           "        cod_impuesto_vta_fac, cod_clasif_vta_fac, cod_esquema_vta_fac, "
                                           "        cod_empresa_vta_fac, ind_tipo_regimen_con, imp_coste"
                                           "   from hbgdwc.dwc_bok_t_canco_hotel_circuit) "
                                           "     AS dwc_bok_t_canco_hotel_circuit",
          "dwc_bok_t_canco_other": "(select seq_rec, seq_reserva, ind_tipo_regimen_fac, imp_margen_canal, "
                                   "        imp_margen_canco, imp_venta, ind_tipo_imp_vta_fac, cod_impuesto_vta_fac, "
                                   "        cod_clasif_vta_fac, cod_esquema_vta_fac, cod_empresa_vta_fac,"
                                   "        ind_tipo_regimen_con, imp_coste"
                                   "   from hbgdwc.dwc_bok_t_canco_other) AS dwc_bok_t_canco_other",
          "dwc_bok_t_canco_transfer": "(select seq_rec, seq_reserva, ind_tipo_regimen_fac, imp_margen_canal, "
                                      "        imp_margen_canco, imp_venta, ind_tipo_imp_vta_fac,  "
                                      "        cod_impuesto_vta_fac, cod_clasif_vta_fac, cod_esquema_vta_fac, "
                                      "        ind_tipo_regimen_con, imp_coste, "
                                      "        cod_empresa_vta_fac from hbgdwc.dwc_bok_t_canco_transfer)"
                                      "     AS dwc_bok_t_canco_transfer",
          "dwc_bok_t_canco_endowments": "(select seq_rec, seq_reserva, ind_tipo_regimen_fac, imp_margen_canal, "
                                        "        imp_margen_canco, imp_venta, ind_tipo_imp_vta_fac, "
                                        "        cod_impuesto_vta_fac, cod_clasif_vta_fac, cod_esquema_vta_fac, "
                                        "        cod_empresa_vta_fac, ind_tipo_regimen_con, imp_coste"
                                        "   from hbgdwc.dwc_bok_t_canco_endowments)"
                                        "     AS dwc_bok_t_canco_endowments",
          "dwc_bok_t_canco_extra": "(select seq_rec, seq_reserva, ord_extra, ind_tipo_regimen_fac, imp_margen_canal, "
                                   "        imp_margen_canco, imp_venta, ind_tipo_imp_vta_fac, cod_impuesto_vta_fac, "
                                   "        cod_clasif_vta_fac, cod_esquema_vta_fac, cod_empresa_vta_fac,"
                                   "        ind_tipo_regimen_con, imp_coste" 
                                   "   from hbgdwc.dwc_bok_t_canco_extra) as dwc_bok_t_canco_extra",
          "dwc_bok_t_extra": "(select grec_seq_rec, rres_seq_reserva, ord_extra, num_bono, cod_interface "
                             "   from hbgdwc.dwc_bok_t_extra) AS dwc_bok_t_extra",
          "dwc_cli_dir_t_cd_discount_bond" : "(select num_bono, cod_interface, cod_campana "
                                             "   from hbgdwc.dwc_cli_dir_t_cd_discount_bond) "
                                             "AS dwc_cli_dir_t_cd_discount_bond",
          "dwc_cli_dir_t_cd_campaign": "(select cod_campana, cod_interface, ind_rentabilidad "
                                       "   from hbgdwc.dwc_cli_dir_t_cd_campaign) AS dwc_cli_dir_t_cd_campaign",

          "dwc_bok_t_cost": "(select sdiv_cod_divisa, fec_desde, fec_hasta, nro_unidades, nro_pax, ind_tipo_unid, "
                            "        ind_p_s, imp_unitario, grec_seq_rec, rres_seq_reserva, ind_tipo_registro, "
                            "        ind_facturable, ind_contra_apunte, rvp_ind_tipo_imp, rvp_cod_impuesto, "
                            "        rvp_cod_esquema, rvp_cod_clasif, rvp_cod_empresa, rext_ord_extra "
                            "   from hbgdwc.dwc_bok_t_cost) AS dwc_bok_t_cost",









          # query functions
          "acq_atlas_general_gn_t_divisa": "(SELECT decimales, cod_divisa "
                                           "   FROM hbgdwc.acq_atlas_general_gn_t_divisa) "
                                           "as acq_atlas_general_gn_t_divisa",

          "acq_atlas_recep_re_t_ge_empresa_tipocambio": "(SELECT tipo_cambio_rex, tipo_cambio_caj, tipo_cambio_liq, "
                                                        "        tipo_cambio_ctb, semp_cod_emp, fec_aplicacion"
                                                        "   FROM hbgdwc."
                                                        ") as acq_atlas_recep_re_t_ge_empresa_tipocambio",
          "": "",
          "": "",
          "acq_atlas_recep_re_t_fc_cambio_divisa": "(SELECT cod_divisa_ori, cod_divisa_des, nro_tasa, "
                                                   "        fec_desde, seq_ttoo"
                                                   "   FROM hbgdwc.acq_atlas_recep_re_t_fc_cambio_divisa) "
                                                   "as cambio_divisa",
          "acq_atlas_recep_re_t_ad_spread": "(SELECT pct_venta, pct_compra, pct_fixing, tipo_cambio, cod_div_origen, "
                                            "        cod_div_destino, fec_aplicacion"
                                            "   FROM hbgdwc.acq_atlas_recep_re_t_ad_spread) "
                                            "as re_t_ad_spread",



          }

'''

hbgdwc.dwc_bok_t_booking_information

table = "(select fec_creacion fec_creacion_info," \
        "       seq_rec sec_rec_info," \
        "       seq_reserva seq_reserva_info" \
        "  FROM hbgdwc.dwc_bok_t_booking_information" \
        " where tipo_op = 'A') as dwc_bok_t_booking_information"



#
hbgdwc.dwc_mtd_t_hotel
hbgdwc.dwc_bok_t_hotel_sale
hbgdwc.dwc_itn_t_internet_destination_id

select izge_cod_destino, seq_hotel from hbgdwc.dwc_mtd_t_hotel
select grec_Seq_rec, rres_seq_reserva, izge_cod_destino, fec_cancelacion from hbgdwc.dwc_bok_t_hotel_sale
select nom_destino, ides_cod_destino, sidi_cod_idioma from hbgdwc.dwc_itn_t_internet_destination_id
        

#
hbgdwc.dwc_bok_t_other
hbgdwc.dwc_con_t_contract_other
-hbgdwc.dwc_itn_t_internet_destination_id

select seq_rec, seq_reserva, nom_contrato, ind_tipo_otro, fec_desde from hbgdwc.dwc_bok_t_other
select cod_destino, seq_rec, nom_contrato, ind_tipo_otro, fec_desde, fec_hasta from hbgdwc.dwc_con_t_contract_other

#
hbgdwc.dwc_bok_t_sale

select fec_desde, fec_hasta, nro_unidades, nro_pax, ind_tipo_unid, ind_p_s, imp_unitario 
       grec_seq_rec, rres_seq_reserva, ind_tipo_registro, Ind_Facturable, ind_contra_apunte,
       imp_impuesto, ind_tipo_regimen,
       Rvp_Ind_Tipo_Imp, Rvp_Cod_Impuesto, Rvp_Cod_Esquema, Rvp_Cod_Clasif, Rvp_Cod_Empresa
  from hbgdwc.dwc_bok_t_sale


# LINE 200
hbgdwc.dwc_oth_v_re_v_impuesto_sap

select pct_impuesto, Ind_Tipo_Imp, Cod_Impuesto, Cod_Esquema, Cod_Clasif, Cod_Emp_Atlas
  from hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP (two queries)

------------------------------------------------BLOCK
#
hbgdwc.dwc_bok_t_canco_hotel

select seq_rec, seq_reserva, ind_tipo_regimen_fac, imp_margen_canal, imp_margen_canco, imp_venta, ind_tipo_imp_vta_fac,
       cod_impuesto_vta_fac, cod_clasif_vta_fac, cod_esquema_vta_fac, cod_empresa_vta_fac
  from hbgdwc.dwc_bok_t_canco_hotel


#
hbgdwc.dwc_oth_v_re_v_impuesto_sap

select pct_impuesto, ind_tipo_imp, cod_impuesto, cod_clasif, cod_esquema, cod_emp_atlas
  from hbgdwc.dwc_oth_v_re_v_impuesto_sap VTA (same as IMP)

select pct_impuesto, ind_tipo_imp, cod_impuesto, cod_clasif, cod_esquema, cod_emp_atlas
  from hbgdwc.dwc_oth_v_re_v_impuesto_sap CPA (same as IMP)


#
hbgdwc.dwc_bok_t_canco_hotel_circuit

select seq_rec, seq_reserva, ind_tipo_regimen_fac, imp_margen_canal, imp_margen_canco, imp_venta, ind_tipo_imp_vta_fac
       cod_impuesto_vta_fac, cod_clasif_vta_fac, cod_esquema_vta_fac, cod_empresa_vta_fac
  from hbgdwc.dwc_bok_t_canco_hotel_circuit
  
#
hbgdwc.dwc_oth_v_re_v_impuesto_sap
SAME AS PREVIOUS VTA & CTA

#
hbgdwc.dwc_bok_t_canco_other

select seq_rec, seq_reserva, ind_tipo_regimen_fac, imp_margen_canal, imp_margen_canco, imp_venta, ind_tipo_imp_vta_fac
       cod_impuesto_vta_fac, cod_clasif_vta_fac, cod_esquema_vta_fac, cod_empresa_vta_fac
  from hbgdwc.dwc_bok_t_canco_other

#
hbgdwc.dwc_oth_v_re_v_impuesto_sap
SAME AS PREVIOUS VTA & CTA
  
#
hbgdwc.dwc_bok_t_canco_transfer

select seq_rec, seq_reserva, ind_tipo_regimen_fac, imp_margen_canal, imp_margen_canco, imp_venta, ind_tipo_imp_vta_fac
       cod_impuesto_vta_fac, cod_clasif_vta_fac, cod_esquema_vta_fac, cod_empresa_vta_fac
  from hbgdwc.dwc_bok_t_canco_transfer

#
hbgdwc.dwc_oth_v_re_v_impuesto_sap
SAME AS PREVIOUS VTA & CTA

#
hbgdwc.dwc_bok_t_canco_endowments

select seq_rec, seq_reserva, ind_tipo_regimen_fac, imp_margen_canal, imp_margen_canco, imp_venta, ind_tipo_imp_vta_fac
       cod_impuesto_vta_fac, cod_clasif_vta_fac, cod_esquema_vta_fac, cod_empresa_vta_fac
  from hbgdwc.dwc_bok_t_canco_endowments

#
hbgdwc.dwc_oth_v_re_v_impuesto_sap
SAME AS PREVIOUS VTA & CTA

#
hbgdwc.dwc_bok_t_canco_extra

select seq_rec, seq_reserva, ord_extra, ind_tipo_regimen_fac, imp_margen_canal, imp_margen_canco, imp_venta, ind_tipo_imp_vta_fac
       cod_impuesto_vta_fac, cod_clasif_vta_fac, cod_esquema_vta_fac, cod_empresa_vta_fac
  from hbgdwc.dwc_bok_t_canco_extra

#
hbgdwc.dwc_oth_v_re_v_impuesto_sap
SAME AS PREVIOUS VTA & CTA

#
hbgdwc.dwc_bok_t_extra
hbgdwc.dwc_cli_dir_t_cd_discount_bond
hbgdwc.dwc_cli_dir_t_cd_campaign

select grec_seq_rec, rres_seq_reserva, ord_extra, num_bono, cod_interface
  from hbgdwc.dwc_bok_t_extra

select num_bono, cod_interface, cod_campana
  from hbgdwc.dwc_cli_dir_t_cd_discount_bond
  
select cod_campana, cod_interface, ind_rentabilidad
  from hbgdwc.dwc_cli_dir_t_cd_campaign
 where ind_rentabilidad = 'N'


#
hbgdwc.dwc_bok_t_cost

select sdiv_cod_divisa, fec_desde, fec_hasta, nro_unidades, nro_pax, ind_tipo_unid, ind_p_s, imp_unitario,
       grec_seq_rec, rres_seq_reserva, ind_tipo_registro, ind_facturable,
       # second select line 2730
       ind_contra_apunte, rvp_ind_tipo_imp, rvp_cod_impuesto, rvp_cod_esquema, rvp_cod_clasif, rvp_cod_empresa
       
  from hbgdwc.dwc_bok_t_cost

#
#
hbgdwc.dwc_bok_t_extra
hbgdwc.dwc_cli_dir_t_cd_discount_bond
hbgdwc.dwc_cli_dir_t_cd_campaign
(SAME QUERIES AS LASTS)

----------------------------------END BLOCK

# 
SAME PREVIOUS BLOCK  (here uses also imp_margen_canco)

REPEAT QUERIES ***


#
hbgdwc.dwc_bok_t_booking_information
(THE SAME AS THE FIRST QUERY)

#
hbgdwc.dwc_cry_t_cancellation_recovery_res_resale

select ind_status, seq_rec, seq_reserva
  from hbgdwc.dwc_cry_t_cancellation_recovery_res_resale

#
hbgdwc.dwc_bok_t_commission

select imp_com_age, imp_comision_imp, seq_rec, seq_reserva
  from hbgdwc.dwc_bok_t_commission 
  
SAME QUERY (here uses imp_comision_imp)


#
hbgdwc.dwc_bok_t_cost
SAME AS PREVIOUS
QUERIES . . .
.............


#hbgdwc.dwc_bok_t_hotel_sale
SAMES AS FIRST with fec_cancelacion


--hbgdwc.dwc_bok_t_booking
--hbgdwc.dwc_mtd_t_ttoo
--hbgdwc.dwc_mtd_t_receptive
--hbgdwc.dwc_itf_t_fc_interface
--hbgdwc.dwc_gen_t_general_country




############ ALL  TABLES ################


hbgdwc.dwc_bok_t_booking_information

hbgdwc.dwc_mtd_t_hotel
hbgdwc.dwc_bok_t_hotel_sale
hbgdwc.dwc_itn_t_internet_destination_id

hbgdwc.dwc_bok_t_other
hbgdwc.dwc_con_t_contract_other

hbgdwc.dwc_bok_t_sale
hbgdwc.dwc_oth_v_re_v_impuesto_sap

hbgdwc.dwc_bok_t_canco_hotel

hbgdwc.dwc_bok_t_canco_hotel_circuit

hbgdwc.dwc_bok_t_canco_other

hbgdwc.dwc_bok_t_canco_transfer

hbgdwc.dwc_bok_t_canco_endowments

hbgdwc.dwc_bok_t_canco_extra

hbgdwc.dwc_bok_t_extra
hbgdwc.dwc_cli_dir_t_cd_discount_bond
hbgdwc.dwc_cli_dir_t_cd_campaign

hbgdwc.dwc_bok_t_cost

hbgdwc.dwc_cry_t_cancellation_recovery_res_resale

hbgdwc.dwc_bok_t_commission

--hbgdwc.dwc_bok_t_booking
--hbgdwc.dwc_mtd_t_ttoo
--hbgdwc.dwc_mtd_t_receptive
--hbgdwc.dwc_itf_t_fc_interface
--hbgdwc.dwc_gen_t_general_country
'''