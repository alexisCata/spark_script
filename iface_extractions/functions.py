from manager.spark_manager import SparkSession
from settings import URL, DRIVER
from tables import tables

# secret_settings
user = "kpiuser"
password = "Kpiuser00"


EXCHANGE_EXC = 'R'
EXCHANGE_RES = 'R'
EXCHANGE_LIQ = 'L'
EXCHANGE_CAJ = 'C'
EXCHANGE_CTB = 'A'

EXCHANGE_TYPES = [EXCHANGE_EXC, EXCHANGE_RES, EXCHANGE_LIQ, EXCHANGE_CAJ, EXCHANGE_CTB]


def load_dataframe(table):
    session = SparkSession.builder.getOrCreate()
    dataframe = session.read.jdbc(url=URL,
                                  table=table,
                                  properties={"driver": DRIVER,
                                              "user": user,
                                              "password": password})
    return dataframe


# Re_Fu_Calcular_Noch_Imp
def calculate_night_amount(start_date, end_date, number_units, number_passengers, service_unit_type, indicator_ps,
                           unit_amount, number_days=None):
    """
    It calculates de number of days or night and the total amount considering the dates, the units and service type.
    Note: 
        If number_days is not null it will not be calculated again and we are calculating the total amount
        If indicator_ps is equal to 'T' the unit_amount must be per day
    :param start_date: Service start date
    :param end_date: Sercie end date
    :param number_units: Number of units (passengers) of the service
    :param number_passengers: Number of passengers
    :param service_unit_type: Service unit type : 'D'ay, 'N'ight, 'P'unctual
    :param indicator_ps: Indicator of 'P'ax or 'S'ervice
    :param unit_amount: Unit amount
    :param number_days: Number of days/night of the service
    :return: number_days, total_amount
    """

    if not start_date or not end_date:
        return 0

    if not number_days:
        if service_unit_type == 'N':
            number_days = (end_date.date() - start_date.date()).days
        elif service_unit_type == 'S':
            number_days = (end_date.date() - start_date.date()).days + 1
        else:
            number_days = 1

    aux_amount = unit_amount * number_days

    total_amount = aux_amount * number_passengers if indicator_ps == 'P' else aux_amount * number_units

    # return number_days, total_amount
    return total_amount


def round_ccy(amount, ccy):
    """
    It round an amount getting the number of decimals from acq_atlas_general_gn_t_divisa
    :param amount: 
    :param ccy: 
    :return: 
    """

    try:
        df = load_dataframe(tables['acq_atlas_general_gn_t_divisa'])
        # df = manager.get_dataframe(tables['acq_atlas_general_gn_t_divisa'])
        decimals = df.filter(df.cod_divisa == ccy).select("decimales").collect()[0][0]
        # print ('amount: ', amount)
        # print ('decimals: ', decimals)
        rounded_amount = round(amount, decimals)
        # print ('rounded_amount: ', rounded_amount)
    except Exception as e:
        print ("Error on round_ccy: {}".format(e.message))
        rounded_amount = amount

    return rounded_amount


def exchange_type_company(company, exchange_type, exchange_date):

    try:
        manager = None # HOW TO DO THAT
        df = manager.get_dataframe(tables['acq_atlas_recep_re_t_ge_empresa_tipocambio'])
        df = df.filter(df.semp_cod_emp == company).filter(df.fec_aplicacion >= exchange_date)\
            .orderBy(df.fec_aplicacion, df.fec_aplicacion.desc())

        cod = df.selectExpr("CASE WHEN '{exchange_type}' = 'R' THEN tipo_cambio_rex"
                            "     WHEN '{exchange_type}' = 'C' THEN tipo_cambio_caj"
                            "     WHEN '{exchange_type}' = 'L' THEN tipo_cambio_liq"
                            "     WHEN '{exchange_type}' = 'A' THEN tipo_cambio_ctb"
                            "     ELSE 'ERROR' END AS cod_exchange_type".format(exchange_type=exchange_type)).collect()[0][0]

    except Exception as e:
        print ("Error on exchange_type_company: {}".format(e.message))
        cod = 'ERROR'

    return cod


def rate_change(company, exchange_type, exchange_date, from_ccy, to_ccy, amount, partner, direct_change):
    """
    It returns the direct change from a ccy to another ccy
    :param company: 
    :param exchange_type: 
    :param exchange_date: 
    :param from_ccy: 
    :param to_ccy: 
    :param amount: 
    :param partner: 
    :param direct_change: 
    :return: 
    """
    rate = 0
    if (exchange_type and exchange_type not in EXCHANGE_TYPES) \
            or (not exchange_type and not direct_change) or not from_ccy or not to_ccy or not exchange_date or \
            (not direct_change and not company):
        raise Exception("Error de sistema, modificar la llamada a la funcion de cambio.")

    if from_ccy == to_ccy:
        rate = 1
    else:
        if not partner:
            if not direct_change:
                cod_exchange_type = exchange_type_company(company, exchange_type, exchange_date)

                # CONSULTA falta tabla re_v_cambio_divisa   c_cambio

            else:
                pass
                # CONSULTA sap_tcurr 2 tablas en DB  c_cambio_ttoo
        else:
            manager = None  # HOW TO DO THAT
            df = manager.get_dataframe(tables['acq_atlas_recep_re_t_fc_cambio_divisa'])
            df = df.filter(df.fec_desde <= exchange_date &
                           df.seq_ttoo == partner &
                           ((df.cod_divisa_ori == from_ccy & df.cod_divisa_des == to_ccy) |
                           (df.cod_divisa_ori == to_ccy & df.cod_divisa_des == from_ccy)))\
                .orderBy(df.fec_desde.desc(), df.cod_divisa_ori == from_ccy and 1 or 2)

            rate = df.selectExpr("CASE WHEN cod_divisa_ori = '{}' THEN nro_tasa"
                                 "     ELSE 1/nro_tasa END AS nro_tasa".format(from_ccy)).collect()[0][0]

    return rate


def direct_exchange(company, exchange_type, exchange_date, from_ccy, to_ccy, amount, partner, direct_change):
    """
    It returns the direct change
    :param company: 
    :param exchange_type: 
    :param exchange_date: 
    :param from_ccy: 
    :param to_ccy: 
    :param amount: 
    :param partner: 
    :param direct_change: 
    :return: 
    """
    rate = rate_change(company, exchange_type, exchange_date, from_ccy, to_ccy, amount, partner, direct_change)

    rate_amount = amount * rate

    return rate_amount


def direct(company, exchange_type, exchange_date, from_ccy, to_ccy, amount,partner, final_direct_change):
    """
    It returns direct exchange
    :param company: 
    :param exchange_type: 
    :param exchange_date: 
    :param from_ccy: 
    :param to_ccy: 
    :param amount: 
    :param partner: 
    :param final_direct_change: 
    :return: 
    """

    rate = rate_change(company, exchange_type, exchange_date, from_ccy, to_ccy, amount, partner, final_direct_change)

    rate_amount = amount * rate

    return rate_amount


def indirect(company, exchange_type, exchange_date, from_ccy, to_ccy, amount,partner, final_direct_change):
    rate_amount = 0
    return rate_amount


# def currency_exchange(company, exchange_type, exchange_date, from_ccy, to_ccy, amount,
#                       round='S', partner=None, direct_change=None, spread='F'):
def currency_exchange(company, exchange_type, exchange_date, from_ccy, amount=None, to_ccy=None,
                      round='S', partner=None, direct_change=None, spread='F'):
    """
    It returns the change from one ccy to another rounding
    :param company: company to search the change
    :param exchange_type: 
    :param exchange_date: 
    :param from_ccy: origin currency
    :param to_ccy: destination currency
    :param amount: amount to change
    :param round: flag to round or not ('S', 'N')
    :param partner: indicates if the change is must be done with partner table
    :param direct_change: change when there is TT.OO
    :param spread: spread type (V:Venta, F:Fixing, C:Compra)
    :return: number
    """
    if not to_ccy:
        to_ccy = from_ccy
    # final_direct_change = None
    rate_amount = None
    if amount:
        rate_amount = amount
    # if amount:
    #     if from_ccy != to_ccy:
    #         if not direct_change:
    #             if not exchange_type:
    #                 raise Exception("Error de sistema, modificar la llamada a la funcion de cambio.")
    #             final_direct_change = exchange_type_company(company, exchange_type, exchange_date)
    #
    #         else:
    #             final_direct_change = exchange_type
    #
    #         try:
    #             rate_amount = direct(company, exchange_type, exchange_date, from_ccy, to_ccy,
    #                                  amount,partner, final_direct_change)
    #         except Exception as e:
    #             rate_amount = indirect(company, exchange_type, exchange_date, from_ccy, to_ccy,
    #                                    amount,partner, final_direct_change)
    #
    #         ###
    #         manager = None
    #         df = manager.get_dataframe(tables['acq_atlas_recep_re_t_ad_spread'])
    #
    #         df = df.filter(df.tipo_cambio == final_direct_change &
    #                        df.cod_divisa_orien == from_ccy &
    #                        df.cod_divisa_destino == to_ccy &
    #                        df.fec_aplicacion <= exchange_date)
    #         try:
    #             pct_spread = df.selectExpr("CASE WHEN '{spread}' = 'V' THEN pct_venta THEN"
    #                                        "     WHEN '{spread}' = 'C' THEN pct_compra THEN"
    #                                        "     WHEN '{spread}' = 'F' THEN pct_fixing THEN"
    #                                        "     ELSE 0 AS pct_spread".format(spread=spread)).collect()[0][0]
    #         except Exception as e:
    #             pct_spread = 0
    #
    #         if pct_spread:
    #             rate_amount = rate_amount * (1 + pct_spread / 100)
    #
    #         if round == 'S':
    #             rate_amount = round_ccy(rate_amount, to_ccy)
    #     else:
    #         if round == 'S':
    #             rate_amount = round_ccy(amount, to_ccy)
    #         else:
    #             rate_amount = amount

    return rate_amount
