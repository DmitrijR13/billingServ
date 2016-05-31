using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.OracleClient;
using System.IO;
using Npgsql;

namespace ServiceFromBill
{
    // ПРИМЕЧАНИЕ. Команду "Переименовать" в меню "Рефакторинг" можно использовать для одновременного изменения имени класса "Service1" в коде, SVC-файле и файле конфигурации.
    // ПРИМЕЧАНИЕ. Чтобы запустить клиент проверки WCF для тестирования службы, выберите элементы Service1.svc или Service1.svc.cs в обозревателе решений и начните отладку.
    public class Service1 : IService1
    {
        public List<ЛС> GetCounters(string db)
        {
            string database = "";
            switch (db.ToLower())
            {
                case "billauk":
                    database = "billAuk";
                    break;
                case "billtlt":
                    database = "billTlt";
                    break;
                default:
                    database = "billTlt";
                    break;
            }
            DataTable dt = SelectCounters(database);
            List<ЛС> returnData = new List<ЛС>();
            List<Показание> показания = new List<Показание>();
            List<Счетчик> счетчики = new List<Счетчик>();
            List<Услуга> услуги = new List<Услуга>();
            ЛС лс = new ЛС();
            Услуга услуга = new Услуга();
            Счетчик счетчик = new Счетчик();
            Показание показание = new Показание();
            string num_ls = "";
            string serv = "";
            string counter = "";
            string counter_num = "";
            try
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    if (num_ls != dt.Rows[i][2].ToString().Trim())
                    {
                        if (i != 0)
                        {
                            счетчик.Показания = показания;
                            показания = new List<Показание>();
                            счетчики.Add(счетчик);
                            счетчик = new Счетчик();
                            услуга.Счетчики = счетчики;
                            счетчики = new List<Счетчик>();
                            услуги.Add(услуга);
                            услуга = new Услуга();
                            лс.Услуги = услуги;
                            услуги = new List<Услуга>(); 
                            returnData.Add(лс);
                            лс = new ЛС();
                        }
                        num_ls = dt.Rows[i][2].ToString().Trim();
                        serv = dt.Rows[i][3].ToString().Trim();
                        counter = dt.Rows[i][4].ToString().Trim();
                        string cnt_num = "";
                        int j = i;
                        while (dt.Rows[j][4].ToString() == counter)
                        {
                            if (dt.Rows[j][5].ToString() != "")
                                cnt_num = dt.Rows[j][5].ToString();
                            j++;
                            if (j == dt.Rows.Count)
                                break;
                        }
                        лс.Адресс = dt.Rows[i][0].ToString().Trim();
                        лс.ФИО = dt.Rows[i][1].ToString().Trim();
                        лс.Номер_ЛС = dt.Rows[i][2].ToString().Trim();

                        услуга.Наименование = dt.Rows[i][3].ToString().Trim();
                        
                        счетчик.ID = dt.Rows[i][4].ToString().Trim();
                        счетчик.Дата_закрытия = dt.Rows[i][9].ToString().Trim();
                        счетчик.Номер = cnt_num;

                        показание = new Показание();
                        показание.Год = dt.Rows[i][8].ToString().Trim();
                        показание.Месяц = dt.Rows[i][6].ToString().Trim();
                        показание.Значение = dt.Rows[i][7].ToString().Trim();
                        показания.Add(показание);
                    }
                    else
                    {
                        if (serv != dt.Rows[i][3].ToString().Trim())
                        {
                            счетчик.Показания = показания;
                            показания = new List<Показание>();
                            счетчики.Add(счетчик);
                            счетчик = new Счетчик();
                            услуга.Счетчики = счетчики;
                            счетчики = new List<Счетчик>();
                            услуги.Add(услуга);
                            услуга = new Услуга();

                            serv = dt.Rows[i][3].ToString().Trim();
                            counter = dt.Rows[i][4].ToString().Trim();
                            counter_num = dt.Rows[i][5].ToString().Trim();
                            string cnt_num = "";
                            int j = i;
                            while (dt.Rows[j][4].ToString() == counter)
                            {
                                if (dt.Rows[j][5].ToString() != "")
                                    cnt_num = dt.Rows[j][5].ToString();
                                j++;
                                if (j == dt.Rows.Count)
                                    break;
                            }


                            услуга.Наименование = dt.Rows[i][3].ToString().Trim();

                            счетчик.ID = dt.Rows[i][4].ToString().Trim();
                            счетчик.Дата_закрытия = dt.Rows[i][9].ToString().Trim();
                            счетчик.Номер = cnt_num;

                            показание = new Показание();
                            показание.Год = dt.Rows[i][8].ToString().Trim();
                            показание.Месяц = dt.Rows[i][6].ToString().Trim();
                            показание.Значение = dt.Rows[i][7].ToString().Trim();
                            показания.Add(показание);
                        }
                        else
                        {
                            if (counter != dt.Rows[i][4].ToString().Trim())
                            {
                                счетчик.Показания = показания;
                                показания = new List<Показание>();
                                счетчики.Add(счетчик);
                                счетчик = new Счетчик();

                                counter = dt.Rows[i][4].ToString().Trim();
                                string cnt_num = "";
                                int j = i;
                                while (dt.Rows[j][4].ToString() == counter)
                                {
                                    if (dt.Rows[j][5].ToString() != "")
                                        cnt_num = dt.Rows[j][5].ToString();
                                    j++;
                                    if (j == dt.Rows.Count)
                                        break;
                                }

                                счетчик.ID = dt.Rows[i][4].ToString().Trim();
                                счетчик.Дата_закрытия = dt.Rows[i][9].ToString().Trim();
                                счетчик.Номер = cnt_num;

                                показание = new Показание();
                                показание.Год = dt.Rows[i][8].ToString().Trim();
                                показание.Месяц = dt.Rows[i][6].ToString().Trim();
                                показание.Значение = dt.Rows[i][7].ToString().Trim();
                                показания.Add(показание);
                            }
                            else
                            {
                                показание = new Показание();
                                показание.Год = dt.Rows[i][8].ToString().Trim();
                                показание.Месяц = dt.Rows[i][6].ToString().Trim();
                                показание.Значение = dt.Rows[i][7].ToString().Trim();
                                показания.Add(показание);
                            }
                        }

                    }

                }
                счетчик.Показания = показания;
                счетчики.Add(счетчик);
                услуга.Счетчики = счетчики;
                услуги.Add(услуга);
                лс.Услуги = услуги;
                returnData.Add(лс);
            }
            catch
            {

            }

            return returnData;
        }

        public List<Payment> GetPayments(string db, bool all = false, string dateFrom = "", string dateTo = "")
        {
            string database = "";
            switch (db.ToLower())
            {
                case "billauk":
                    database = "billAuk";
                    break;
                case "billtlt":
                    database = "billTlt";
                    break;
                default:
                    database = "billTlt";
                    break;
            }
            DataTable dt = SelectPayments(database, all, dateFrom, dateTo);
            List<Payment> returnData = new List<Payment>();
            try
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    Payment payment = new Payment();
                    payment.Bank = dt.Rows[i][4].ToString().Trim();
                    payment.NumLs = dt.Rows[i][0].ToString().Trim();
                    payment.PaymentDate = dt.Rows[i][3].ToString().Trim();
                    payment.PaymentSum = dt.Rows[i][2].ToString().Trim();
                    payment.Pkod = dt.Rows[i][1].ToString().Trim();
                    returnData.Add(payment);
                }
            }
            catch
            {

            }

            return returnData;
        }

        public void LoadForGis(String db, Int32 year, Int32 month)
        {
            StreamWriter sw = new StreamWriter(@"C:\temp\ImportGisLog.txt", true);
            String database = "";
            String address = "";
            List<Int32> roIdList = new List<int>();
            List<String> addressNotFound = new List<string>();
            Dictionary<string, string> fromAddressAndPkodToHcsId = new Dictionary<string, string>();
            switch (db.ToLower())
            {
                case "billauk":
                    database = "billAuk";
                    break;
                case "billtlt":
                    database = "billTlt";
                    break;
                default:
                    database = "billTlt";
                    break;
            }

            #region Update/Insert HCS_HOUSE_ACCOUNT
            DataTable dt = SelectLSInfo(database);
            try
            {
                for(int i = 0; i < dt.Rows.Count; i++)
                {
                    if (address != dt.Rows[i][0].ToString())
                    {
                        address = dt.Rows[i][0].ToString();
                        if (address.Contains("22 ПАРТСЪЕЗДА"))
                            address = address.Replace("22 ПАРТСЪЕЗДА", "Двадцать второго Партсъезда");
                        roIdList = GetRoId(address);
                    }
                    if (roIdList != null && roIdList.Count > 0)
                    {
                        String hcsHouseAccountIdText = "0";
                        Int32 hcsHouseAccountId = 0;
                        foreach (var id in roIdList)
                        {
                            hcsHouseAccountIdText = GetHcsHouseAccountId(Convert.ToInt32(dt.Rows[i][2]), id);
                            if (hcsHouseAccountIdText.Split('|')[0] == "НАЙДЕНО")
                            {
                                if (hcsHouseAccountIdText.Split('|')[1] != "0")
                                {
                                    hcsHouseAccountId = Convert.ToInt32(hcsHouseAccountIdText.Split('|')[1]);
                                    fromAddressAndPkodToHcsId.Add(address + "|" + Convert.ToInt32(dt.Rows[i][2]), hcsHouseAccountId + "|" + id);
                                    break;
                                }
                            }
                            else
                            {
                                sw.WriteLine(DateTime.Now + ": Ошибка при поиске ЛС. ОШИБКА = '" + hcsHouseAccountIdText.Split('|')[1] + "'");
                                hcsHouseAccountId = -1;
                            }
                        }
                        if (hcsHouseAccountId == 0)
                        {
                            if (roIdList.Count == 1)
                            {
                                String res = InsertHcsHouseAccount(roIdList[0], dt.Rows[i][2].ToString(), dt.Rows[i][1].ToString(), dt.Rows[i][3].ToString(),
                                    dt.Rows[i][4].ToString(), dt.Rows[i][5].ToString(), dt.Rows[i][6].ToString(),
                                    dt.Rows[i][7].ToString(), dt.Rows[i][8].ToString(), dt.Rows[i][9].ToString());
                                if (res.Split('|')[0] != "ЗАГРУЖЕНО")
                                {
                                    sw.WriteLine(DateTime.Now + ": Ошибка при обновлении информации по лс. Адрес = '" +
                                                 address +
                                                 "'. Квартира = '" + dt.Rows[i][1].ToString() + "'. Платежный код = '" +
                                                 dt.Rows[i][2].ToString() + "'. ОШИБКА = " + res);
                                }
                                else
                                {
                                    hcsHouseAccountId = Convert.ToInt32(res.Split('|')[1]);
                                    fromAddressAndPkodToHcsId.Add(address + "|" + Convert.ToInt32(dt.Rows[i][2]), hcsHouseAccountId + "|" + roIdList[0]);
                                }
                            }
                            else
                            {
                                sw.WriteLine(DateTime.Now + ": Невозможно одназначно определить адрес. Адрес = '" + address +
                                             "'. Квартира = '" + dt.Rows[i][1].ToString() + "'. Платежный код = '" +
                                             dt.Rows[i][2].ToString() + "'");
                            }
                        }
                        else if (hcsHouseAccountId == -1)
                        {
                            
                        }
                        else
                        {
                            String res = UpdateHcsHouseAccount(hcsHouseAccountId, dt.Rows[i][1].ToString(), dt.Rows[i][3].ToString(),
                                dt.Rows[i][4].ToString(), dt.Rows[i][5].ToString(), dt.Rows[i][6].ToString(),
                                dt.Rows[i][7].ToString(), dt.Rows[i][8].ToString(), dt.Rows[i][9].ToString());
                            if (res != "ЗАГРУЖЕНО")
                                sw.WriteLine(DateTime.Now + ": Ошибка при обновлении информации по лс. Адрес = '" + address +
                                             "'. Квартира = '" + dt.Rows[i][1].ToString() + "'. Платежный код = '" +
                                             dt.Rows[i][2].ToString() + "'. ОШИБКА = " + res);
                        }
                    }
                    else
                    {
                        if (!addressNotFound.Contains(address))
                        {
                            sw.WriteLine(DateTime.Now + ": Не найден данный адрес. Адрес = '" + address + "'");
                            addressNotFound.Add(address);
                        }                      
                    }                   
                }
            }
            catch(Exception ex)
            {
                sw.WriteLine(DateTime.Now + ": Ошибка при обработке. Шаг 1. ОШИБКА = '" + ex.ToString() + "'");
            }
            #endregion

            #region Update/Insert HCS_HOUSE_ACCOUNT_CHARGE
            DataTable dt2 = SelectLSInfoCharge(database, year, month);
            try
            {
                for (int i = 0; i < dt2.Rows.Count; i++)
                {
                    Int32 roId = 0;
                    Int32 hcsHouseAccountId = 0;
                    String hcsHouseAccountIdChargeText = "0";
                    Int32 hcsHouseAccountChargeId = 0;
                    address = dt2.Rows[i][0].ToString();
                    if (address.Contains("22 ПАРТСЪЕЗДА"))
                        address = address.Replace("22 ПАРТСЪЕЗДА", "Двадцать второго Партсъезда");
                    if (fromAddressAndPkodToHcsId.ContainsKey(address + "|" + Convert.ToInt32(dt2.Rows[i][2])))
                    {
                        roId = Convert.ToInt32(fromAddressAndPkodToHcsId[address + "|" + Convert.ToInt32(dt2.Rows[i][2])].Split('|')[1]);
                        hcsHouseAccountId = Convert.ToInt32(fromAddressAndPkodToHcsId[address + "|" + Convert.ToInt32(dt2.Rows[i][2])].Split('|')[0]);
                        string supp = "";
                        try
                        {
                            supp = dt2.Rows[i][3].ToString().Split('/')[1].Replace('\"', ' ').Trim();
                        }
                        catch
                        {
                            supp = dt2.Rows[i][3].ToString().Split(',')[1].Replace('\"', ' ').Trim();
                        }
                        hcsHouseAccountIdChargeText = GetHcsHouseAccountChargeId(hcsHouseAccountId, roId,
                            dt2.Rows[i][4].ToString(), supp, Convert.ToDateTime(dt2.Rows[i][6].ToString()).ToShortDateString());
                        if (hcsHouseAccountIdChargeText.Split('|')[0] == "НАЙДЕНО")
                        {
                            if (hcsHouseAccountIdChargeText.Split('|')[1] != "0")
                            {
                                hcsHouseAccountChargeId = Convert.ToInt32(hcsHouseAccountIdChargeText.Split('|')[1]);
                                break;
                            }
                        }
                        else
                        {
                            sw.WriteLine(DateTime.Now + ": Ошибка при поиске начислений по ЛС. ОШИБКА = '" + hcsHouseAccountIdChargeText.Split('|')[1] + "'");
                            hcsHouseAccountChargeId = -1;
                        }
                    }
                    if (hcsHouseAccountChargeId == 0)
                    {
                        if (hcsHouseAccountId != 0 && roId != 0)
                        {
                            string supp = "";
                            try
                            {
                                supp = dt2.Rows[i][3].ToString().Split('/')[1].Replace('\"', ' ').Trim();
                            }
                            catch
                            {
                                supp = dt2.Rows[i][3].ToString().Split(',')[1].Replace('\"', ' ').Trim();
                            }
                            String res = InsertHcsHouseAccountCharge(hcsHouseAccountId, roId, dt2.Rows[i][5].ToString(),
                                dt2.Rows[i][7].ToString(), dt2.Rows[i][8].ToString(), dt2.Rows[i][9].ToString(),
                                dt2.Rows[i][10].ToString(), dt2.Rows[i][11].ToString(), dt2.Rows[i][12].ToString(),
                                dt2.Rows[i][13].ToString(), dt2.Rows[i][14].ToString(), dt2.Rows[i][15].ToString(),
                                dt2.Rows[i][4].ToString(),
                                Convert.ToDateTime(dt2.Rows[i][6].ToString()).ToShortDateString(), supp);
                            if (res != "ЗАГРУЖЕНО")
                            {
                                sw.WriteLine(DateTime.Now + ": Ошибка при обновлении информации по лс. Адрес = '" +
                                             address +
                                             "'. Квартира = '" + dt2.Rows[i][1].ToString() + "'. Платежный код = '" +
                                             dt2.Rows[i][2].ToString() + "'. Услуга = '" + dt2.Rows[i][4].ToString() + "'. ОШИБКА = " + res);
                            }
                        }                      
                    }
                    else if (hcsHouseAccountChargeId == -1)
                    {

                    }
                    else
                    {
                        String res = UpdateHcsHouseAccountCharge(hcsHouseAccountChargeId, dt2.Rows[i][5].ToString(),
                            dt2.Rows[i][7].ToString(), dt2.Rows[i][8].ToString(), dt2.Rows[i][9].ToString(),
                            dt2.Rows[i][10].ToString(), dt2.Rows[i][11].ToString(), dt2.Rows[i][12].ToString(),
                            dt2.Rows[i][13].ToString(), dt2.Rows[i][14].ToString(), dt2.Rows[i][15].ToString());
                        if (res != "ЗАГРУЖЕНО")
                            sw.WriteLine(DateTime.Now + ": Ошибка при обновлении информации по лс. Адрес = '" + address +
                                         "'. Квартира = '" + dt2.Rows[i][1].ToString() + "'. Платежный код = '" +
                                         dt2.Rows[i][2].ToString() + "'. Услуга = '" + dt2.Rows[i][4].ToString() + "'. ОШИБКА = " + res);
                    }
                }
            }
            catch (Exception ex)
            {
                sw.WriteLine(DateTime.Now + ": Ошибка при обработке. Шаг 2. ОШИБКА = '" + ex.ToString() + "'");
            }
            #endregion

            #region Insert/Update countersSpis
            DataTable dt3 = SelectLSInfoCounters(database);
            try
            {
                for (int i = 0; i < dt3.Rows.Count; i++)
                {
                    Int32 roId = 0;
                    Int32 hcsHouseAccountId = 0;
                    String hcsHouseAccountIdCountersText = "0";
                    Int32 hcsHouseAccountCountersId = 0;
                    address = dt3.Rows[i][0].ToString();
                    if (address.Contains("22 ПАРТСЪЕЗДА"))
                        address = address.Replace("22 ПАРТСЪЕЗДА", "Двадцать второго Партсъезда");
                    if (fromAddressAndPkodToHcsId.ContainsKey(address + "|" + Convert.ToInt32(dt3.Rows[i][2])))
                    {
                        roId = Convert.ToInt32(fromAddressAndPkodToHcsId[address + "|" + Convert.ToInt32(dt3.Rows[i][2])].Split('|')[1]);
                        hcsHouseAccountId = Convert.ToInt32(fromAddressAndPkodToHcsId[address + "|" + Convert.ToInt32(dt3.Rows[i][2])].Split('|')[0]);
                        hcsHouseAccountIdCountersText = GetHcsHouseAccountCountersId(hcsHouseAccountId, roId,
                            dt3.Rows[i][3].ToString(), dt3.Rows[i][4].ToString());
                        if (hcsHouseAccountIdCountersText.Split('|')[0] == "НАЙДЕНО")
                        {
                            if (hcsHouseAccountIdCountersText.Split('|')[1] != "0")
                            {
                                hcsHouseAccountCountersId = Convert.ToInt32(hcsHouseAccountIdCountersText.Split('|')[1]);
                                break;
                            }
                        }
                        else
                        {
                            sw.WriteLine(DateTime.Now + ": Ошибка при поиске показаний счетчиков по ЛС. ОШИБКА = '" + hcsHouseAccountIdCountersText.Split('|')[1] + "'");
                            hcsHouseAccountCountersId = -1;
                        }
                        if (hcsHouseAccountCountersId == 0)
                        {
                            if (hcsHouseAccountId != 0 && roId != 0)
                            {
                                String res = InsertHcsHouseAccountCounters(hcsHouseAccountId, roId, Convert.ToDateTime(dt2.Rows[i][5].ToString()).ToShortDateString(),
                                Convert.ToDateTime(dt2.Rows[i][6].ToString()).ToShortDateString(), dt2.Rows[i][7].ToString(), dt2.Rows[i][8].ToString(), dt2.Rows[i][9].ToString(),
                                dt2.Rows[i][3].ToString(), dt2.Rows[i][4].ToString());
                                if (res != "ЗАГРУЖЕНО")
                                {
                                    sw.WriteLine(DateTime.Now + ": Ошибка при обновлении информации по лс. Адрес = '" +
                                                 address +
                                                 "'. Квартира = '" + dt2.Rows[i][1].ToString() + "'. Платежный код = '" +
                                                  dt2.Rows[i][2].ToString() + "'. Услуга = '" + dt2.Rows[i][3].ToString() + "'. Счетчик = '" + dt2.Rows[i][4].ToString() +
                                                 "'.ОШИБКА = " + res);
                                }
                            }
                        }
                        else if (hcsHouseAccountCountersId == -1)
                        {

                        }
                        else
                        {
                            String res = UpdateHcsHouseAccountCounters(hcsHouseAccountCountersId, Convert.ToDateTime(dt2.Rows[i][5].ToString()).ToShortDateString(),
                                Convert.ToDateTime(dt2.Rows[i][6].ToString()).ToShortDateString(), dt2.Rows[i][7].ToString(), dt2.Rows[i][8].ToString(), dt2.Rows[i][9].ToString());
                            if (res != "ЗАГРУЖЕНО")
                                sw.WriteLine(DateTime.Now + ": Ошибка при обновлении информации по лс. Адрес = '" + address +
                                             "'. Квартира = '" + dt2.Rows[i][1].ToString() + "'. Платежный код = '" +
                                             dt2.Rows[i][2].ToString() + "'. Услуга = '" + dt2.Rows[i][3].ToString() + "'. Счетчик = '" + dt2.Rows[i][4].ToString() + 
                                             "'.ОШИБКА = " + res);
                        }

                    }
                    
                }
            }
            catch (Exception ex)
            {
                sw.WriteLine(DateTime.Now + ": Ошибка при обработке. Шаг 3. ОШИБКА = '" + ex.ToString() + "'");
            }
            #endregion
            sw.Close();
        }

        private DataTable SelectCounters(string database)
        {
            string connStr = "Server=localhost;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
            //string connStr = "Server=192.168.1.25;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
            string cmdText = @"SELECT ul.ulica || ', д.' || d.ndom || ' кв.'|| k.nkvar, k.fio, " + (database == "billAuk" ? "k.num_ls" : "k.pkod") +
                                @", s.service, c.nzp_counter, c.num_cnt,
                                CASE WHEN Extract(month from dat_uchet) - 1 = 0 THEN 12 ELSE Extract(month from dat_uchet) - 1 END as month,
                                c.val_cnt,
                                CASE WHEN Extract(month from dat_uchet) - 1 = 0 THEN Extract(year from dat_uchet) - 1 ELSE Extract(year from dat_uchet) END as year, c.dat_close 
                                FROM bill01_data.kvar k
                                INNER JOIN bill01_data.dom d on d.nzp_dom = k.nzp_dom
                                INNER JOIN bill01_data.s_ulica ul on d.nzp_ul = ul.nzp_ul
                                INNER JOIN bill01_data.counters c on c.nzp_kvar = k.nzp_kvar
                                INNER JOIN bill01_data.counters_spis cs on cs.nzp_counter = c.nzp_counter
                                INNER JOIN bill01_kernel.services s on s.nzp_serv = c.nzp_serv
                                where ((Extract(month from dat_uchet) >= 10 AND Extract(year from dat_uchet) = 2015) OR Extract(year from dat_uchet) >= 2016)
                                AND (cs.dat_close is null OR cs.dat_close >= current_date) and c.is_actual != 100
                                order by 1,2,4,5,9,7";
            NpgsqlConnection conn = new NpgsqlConnection(connStr);
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);
            NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            try
            {
                da.Fill(dt);
                return dt;
            }
            catch
            {
                return null;
            }
        }

        private DataTable SelectPayments(string database, bool all, string dateFrom, string dateTo)
        {
            string connStr = "Server=localhost;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
            //string connStr = "Server=192.168.1.25;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";

            Int32 yearFrom, yearTo;

            string dateBetween = "";

            if (String.IsNullOrEmpty(dateFrom) && String.IsNullOrEmpty(dateTo) && !all)
            {
                dateBetween += " pl.date_distr::date = '" + DateTime.Now.AddDays(-1).ToShortDateString() + "'::date ";
                yearFrom = all ? 2015 : DateTime.Now.AddDays(-1).Year;
                yearTo = all ? 2016 : DateTime.Now.AddDays(-1).Year;
            }
            else
            {
                dateBetween += String.IsNullOrEmpty(dateFrom)
                    ? " pl.date_distr::date >= '01-01-1900'::date "
                    : " pl.date_distr::date >= '" + Convert.ToDateTime(dateFrom).ToShortDateString() + "'::date ";

                dateBetween += String.IsNullOrEmpty(dateTo)
                    ? " AND pl.date_distr::date <= '01-01-3000'::date "
                    : " AND pl.date_distr::date <= '" + Convert.ToDateTime(dateTo).ToShortDateString() + "'::date ";

                yearFrom = String.IsNullOrEmpty(dateFrom) ? 2015 : Convert.ToDateTime(dateFrom).Year;

                yearTo = String.IsNullOrEmpty(dateTo) ? 2016 : Convert.ToDateTime(dateTo).Year;
            }

            string where = dateBetween;

            string cmdText = "";

            if (yearFrom == yearTo)
            {
                
                cmdText = @"SELECT k.num_ls, k.pkod, sum(pl.g_sum_ls) as g_sum_ls, to_char(pl.dat_vvod, 'dd-mm-yyyy') as dat_vvod,  b.bank as bank
                                FROM fbill_fin_" + (yearFrom - 2000).ToString("00") + @".pack_ls pl 
                                INNER JOIN fbill_fin_" + (yearFrom - 2000).ToString("00") + @".pack p on p.nzp_pack = pl.nzp_pack 
                                INNER JOIN fbill_kernel.s_bank b on b.nzp_bank = p.nzp_bank 
                                INNER JOIN fbill_data.kvar k on k.num_ls = pl.num_ls 
                                INNER JOIN fbill_data.dom d on d.nzp_dom = k.nzp_dom 
                                INNER JOIN fbill_data.s_ulica ul on ul.nzp_ul = d.nzp_ul 
                                where " + where + " group by 1,2,4,5 order by 4,5";
            }
            else
            {               
                for (int i = yearFrom; i <= yearTo; i++)
                {
                    if (i != yearFrom)
                        cmdText += " UNION ALL ";

                    cmdText += @" SELECT k.num_ls, k.pkod, sum(pl.g_sum_ls) as g_sum_ls, to_char(pl.dat_vvod, 'dd-mm-yyyy') as dat_vvod,  b.bank as bank
                            FROM fbill_fin_" + (i - 2000).ToString("00") + @".pack_ls pl 
                            INNER JOIN fbill_fin_" + (i - 2000).ToString("00") + @".pack p on p.nzp_pack = pl.nzp_pack 
                            INNER JOIN fbill_kernel.s_bank b on b.nzp_bank = p.nzp_bank 
                            INNER JOIN fbill_data.kvar k on k.num_ls = pl.num_ls 
                            INNER JOIN fbill_data.dom d on d.nzp_dom = k.nzp_dom 
                            INNER JOIN fbill_data.s_ulica ul on ul.nzp_ul = d.nzp_ul 
                            where " + where + " group by 1,2,4,5 ";
                }
                cmdText += " order by 4,5 ";
            }
            NpgsqlConnection conn = new NpgsqlConnection(connStr);
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);
            NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            try
            {
                da.Fill(dt);
                return dt;
            }
            catch
            {
                return null;
            }
        }

        private DataTable SelectLSInfo(String database)
        {
            //string connStr = "Server=localhost;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
            string connStr = "Server=192.168.1.25;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";

            string cmdText = @"SELECT ulica || ', д.' || ndom as address, nkvar, pkod, p1.val_prm, p2.val_prm, p3.val_prm, p4.val_prm, p5.val_prm, 
                                CASE WHEN is_open = '1' THEN 'Открыт' ELSE 'Закрыт' END as status,
                                CASE WHEN p7.val_prm = '0' THEN 0 ELSE 1 END as privatized
                                FROM fbill_data.kvar k
                                INNER JOIN fbill_data.dom d on d.nzp_dom = k.nzp_dom 
                                INNER JOIN fbill_data.s_ulica u on u.nzp_ul = d.nzp_ul 
                                LEFT JOIN (SELECT nzp, max(val_prm) as val_prm from bill01_data.prm_1 where is_actual = 1 AND Extract(year from dat_po) = 3000 AND nzp_prm = 5 group by 1) p1 on p1.nzp = k.nzp_kvar
                                LEFT JOIN (SELECT nzp, max(val_prm) as val_prm from bill01_data.prm_1 where is_actual = 1 AND Extract(year from dat_po) = 3000 AND nzp_prm = 10 group by 1) p2 on p2.nzp = k.nzp_kvar
                                LEFT JOIN (SELECT nzp, max(val_prm) as val_prm from bill01_data.prm_1 where is_actual = 1 AND Extract(year from dat_po) = 3000 AND nzp_prm = 4 group by 1) p3 on p3.nzp = k.nzp_kvar
                                LEFT JOIN (SELECT nzp, max(val_prm) as val_prm from bill01_data.prm_1 where is_actual = 1 AND Extract(year from dat_po) = 3000 AND nzp_prm = 6 group by 1) p4 on p4.nzp = k.nzp_kvar
                                LEFT JOIN (SELECT nzp, max(val_prm) as val_prm from bill01_data.prm_1 where is_actual = 1 AND Extract(year from dat_po) = 3000 AND nzp_prm = 107 group by 1) p5 on p5.nzp = k.nzp_kvar
                                LEFT JOIN (SELECT nzp, max(val_prm) as val_prm from bill01_data.prm_1 where is_actual = 1 AND Extract(year from dat_po) = 3000 AND nzp_prm = 2009 group by 1) p7 on p7.nzp = k.nzp_kvar
                                where k.nzp_dom in (30883, 7154256)
                                order by 1,2";
         
            NpgsqlConnection conn = new NpgsqlConnection(connStr);
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);
            NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            try
            {
                da.Fill(dt);
                return dt;
            }
            catch
            {
                return null;
            }
        }

        private List<Int32> GetRoId(String address)
        {
            List<Int32> roId = new List<int>();
            string connStr = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=85.140.61.250)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ezhkh)));User Id = b4_gkh_samara; Password = ACTANONVERBA";

            string cmdText = @"SELECT ID FROM GKH_REALITY_OBJECT where replace(lower(GRO.ADDRESS), ' ','') LIKE replace(lower('%" + address + "%'), ' ','') " +
                " and MUNICIPALITY_ID in (21690, 21691, 21692, 21693, 21694, 21695, 21696, 21697, 21698)";

            NpgsqlConnection conn = new NpgsqlConnection(connStr);
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);
            NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            try
            {
                da.Fill(dt);
                if (dt.Rows.Count > 0)
                {
                    for (int i = 0; i < dt.Rows.Count; i++)
                    {
                        roId.Add(Convert.ToInt32(dt.Rows[i][0]));
                    }
                    return roId;
                }
                else
                {
                    return null;                 
                }
                    
            }
            catch (Exception e)
            {
                return null;
            }
        }

        private String GetHcsHouseAccountId(Int32 pkod, Int32 roId)
        {

            string connStr = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=85.140.61.250)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ezhkh)));User Id = b4_gkh_samara; Password = ACTANONVERBA";

            string cmdText = @"SELECT ID FROM HCS_HOUSE_ACCOUNT where PAYMENT_CODE=" + pkod + " AND REALITY_OBJECT_ID = " + roId;

            NpgsqlConnection conn = new NpgsqlConnection(connStr);
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);
            NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            try
            {
                da.Fill(dt);
                if (dt.Rows.Count == 1)
                    return "НАЙДЕНО|" + dt.Rows[0][0].ToString();
                else
                    return "НАЙДЕНО|0";
            }
            catch(Exception e)
            {
                return "ОШИБКА|" + e.ToString();
            }
        }

        private String UpdateHcsHouseAccount(Int32 id, String apartment, String residentsCounts, String temporaryGoneCount, String apartmentArea, String livingArea, String roomsCount, 
                                            String houseStatus, String privatized)
        {
            string connStr = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=85.140.61.250)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ezhkh)));User Id = b4_gkh_samara; Password = ACTANONVERBA";

            if (String.IsNullOrEmpty(apartment) || apartment == " ")
                apartment = "0";
            if (String.IsNullOrEmpty(residentsCounts) || residentsCounts == " ")
                residentsCounts = "0";
            if (String.IsNullOrEmpty(temporaryGoneCount) || temporaryGoneCount == " ")
                temporaryGoneCount = "0";
            if (String.IsNullOrEmpty(apartmentArea) || apartmentArea == " ")
                apartmentArea = "0";
            if (String.IsNullOrEmpty(livingArea) || livingArea == " ")
                livingArea = "0";
            if (String.IsNullOrEmpty(roomsCount) || roomsCount == " ")
                roomsCount = "0";
            if (String.IsNullOrEmpty(privatized) || privatized == " ")
                privatized = "0";

            apartmentArea = apartmentArea.Replace(',', '.');
            livingArea = livingArea.Replace(',', '.');

            string cmdText = @"UPDATE HCS_HOUSE_ACCOUNT 
                                SET APARTMENT = " + apartment +
                                ", RESIDENTS_COUNT = " + residentsCounts +
                                ", TEMPORARY_GONE_COUNT = " + temporaryGoneCount +
                                ", APARTMENT_AREA = " + apartmentArea +
                                ", LIVING_AREA = " + livingArea +
                                ", ROOMS_COUNT = " + roomsCount +
                                ", HOUSE_STATUS = " + houseStatus +
                                ", PRIVATIZED = " + privatized +
                                " WHERE ID = " + id;
            OracleConnection conn = new OracleConnection(connStr);
            OracleCommand cmd1 = new OracleCommand(cmdText, conn);
            conn.Open();
            try
            {
                cmd1.ExecuteNonQuery();
                return "ЗАГРУЖЕНО";
            }
            catch (Exception e)
            {
                return e.ToString();
            }
            finally
            {
                conn.Close();
            }
        }

        private String InsertHcsHouseAccount(Int32 roId, String paymentCode, String apartment, String residentsCounts, String temporaryGoneCount, String apartmentArea, String livingArea, String roomsCount,
                                            String houseStatus, String privatized)
        {
            string connStr = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=85.140.61.250)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ezhkh)));User Id = b4_gkh_samara; Password = ACTANONVERBA";

            if (String.IsNullOrEmpty(apartment) || apartment == " ")
                apartment = "0";
            if (String.IsNullOrEmpty(residentsCounts) || residentsCounts == " ")
                residentsCounts = "0";
            if (String.IsNullOrEmpty(temporaryGoneCount) || temporaryGoneCount == " ")
                temporaryGoneCount = "0";
            if (String.IsNullOrEmpty(apartmentArea) || apartmentArea == " ")
                apartmentArea = "0";
            if (String.IsNullOrEmpty(livingArea) || livingArea == " ")
                livingArea = "0";
            if (String.IsNullOrEmpty(roomsCount) || roomsCount == " ")
                roomsCount = "0";
            if (String.IsNullOrEmpty(privatized) || privatized == " ")
                privatized = "0";

            apartmentArea = apartmentArea.Replace(',', '.');
            livingArea = livingArea.Replace(',', '.');

            string cmdText = @"INSERT INTO HCS_HOUSE_ACCOUNT(ID, OBJECT_VERSION, OBJECT_CREATE_DATE, OBJECT_EDIT_DATE, APARTMENT, RESIDENTS_COUNT,  
								HOUSE_STATUS, APARTMENT_AREA, LIVING_AREA, ROOMS_COUNT, PRIVATIZED,
								REALITY_OBJECT_ID, PAYMENT_CODE, LIVING) 
								VALUES(HIBERNATE_SEQUENCE.NEXTVAL, 0, CURRENT_DATE, CURRENT_DATE, "+ apartment
								+ ","+ residentsCounts +", '"+houseStatus+"', "+ apartmentArea
								+", "+livingArea+", "+roomsCount+", "+privatized+", "+roId
                                + ", " + paymentCode + ", 0) RETURNING id INTO :id";
            OracleConnection conn = new OracleConnection(connStr);
            OracleCommand cmd1 = new OracleCommand(cmdText, conn);
            cmd1.Parameters.Add(new OracleParameter
            {
                ParameterName = ":id",
                OracleType = OracleType.Number,
                Direction = ParameterDirection.Output
            });
            conn.Open();
            try
            {
                cmd1.ExecuteNonQuery();
                int id = Convert.ToInt32(cmd1.Parameters[":id"].Value.ToString());
                return "ЗАГРУЖЕНО|" + id;
            }
            catch (Exception e)
            {
                return e.ToString() + "|0";
            }
            finally
            {
                conn.Close();
            }
        }

        private DataTable SelectLSInfoCharge(String database, Int32 year, Int32 month)
        {
            //string connStr = "Server=localhost;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
            string connStr = "Server=192.168.1.25;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";

            string cmdText = @"SELECT ulica || ', д.' || ndom as address, nkvar, pkod, sup.name_supp, serv.service, c.tarif, '2016-01-01'::date, c.c_calc, c.sum_nedop, c.sum_charge, c.reval, c.sum_insaldo, coalesce(p.sum_rcl, 0) as sum_rcl, 
                                c.sum_money, c.sum_charge + c.reval - c.sum_nedop + coalesce(p.sum_rcl, 0) as charge, c.sum_outsaldo
                                FROM bill01_charge_" + (year - 2000).ToString("00") + ".charge_" + month.ToString("00") + @" c))
                                INNER JOIN fbill_data.kvar k on k.nzp_kvar = c.nzp_kvar
                                INNER JOIN fbill_data.dom d on d.nzp_dom = k.nzp_dom 
                                INNER JOIN fbill_data.s_ulica u on u.nzp_ul = d.nzp_ul 
                                INNER JOIN fbill_kernel.supplier sup on sup.nzp_supp = c.nzp_supp
                                INNER JOIN fbill_kernel.services serv on serv.nzp_serv = c.nzp_serv
                                LEFT JOIN (SELECT nzp_kvar, nzp_serv, sum(sum_rcl) as sum_rcl from bill01_charge_" + (year - 2000).ToString("00") + @".perekidka 
	                                                    where month_ = " + month + @" group by 1,2) p on p.nzp_kvar = c.nzp_kvar and p.nzp_serv = c.nzp_serv
                                where k.nzp_dom in (30883, 7154256) and c.nzp_serv != 1 and c.dat_charge is null and (c.tarif > 0 or c.c_calc > 0 or c.sum_nedop > 0 or c.sum_charge > 0 or c.reval > 0 or c.sum_insaldo > 0 or c.sum_money > 0 
                                or c.sum_money > 0 or coalesce(p.sum_rcl, 0) > 0 or c.sum_charge + c.reval - c.sum_nedop + coalesce(p.sum_rcl, 0) > 0 or c.sum_outsaldo > 0)                               
                                order by 1,2";

            NpgsqlConnection conn = new NpgsqlConnection(connStr);
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);
            NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            try
            {
                da.Fill(dt);
                return dt;
            }
            catch
            {
                return null;
            }
        }

        private String GetHcsHouseAccountChargeId(Int32 accountId, Int32 roId, String service, String supplier, String dateCharging)
        {

            string connStr = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=85.140.61.250)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ezhkh)));User Id = b4_gkh_samara; Password = ACTANONVERBA";

            string cmdText = "SELECT ID FROM HCS_HOUSE_ACCOUNT_CHARGE " +
                            " where REALITY_OBJECT_ID = " + roId +
                            " and ACCOUNT_ID = " + accountId +
                            " and Service = '" + service +
                            "' and SUPPLIER = '" + supplier +
                            "' and DATE_CHARGING = to_date('" + dateCharging + "', 'dd.mm.yyyy')";

            NpgsqlConnection conn = new NpgsqlConnection(connStr);
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);
            NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            try
            {
                da.Fill(dt);
                if (dt.Rows.Count == 1)
                    return "НАЙДЕНО|" + dt.Rows[0][0].ToString();
                else
                    return "НАЙДЕНО|0";
            }
            catch (Exception e)
            {
                return "ОШИБКА|" + e.ToString();
            }
        }

        private String UpdateHcsHouseAccountCharge(Int32 id, String tariff, String expense, String underdelivery, String charged, String recalc, 
            String innerBalance, String changed, String payment, String chargedPayment, String outerBalance)
        {
            string connStr = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=85.140.61.250)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ezhkh)));User Id = b4_gkh_samara; Password = ACTANONVERBA";

            if (String.IsNullOrEmpty(tariff) || tariff == " ")
                tariff = "0";
            if (String.IsNullOrEmpty(expense) || expense == " ")
                expense = "0";
            if (String.IsNullOrEmpty(underdelivery) || underdelivery == " ")
                underdelivery = "0";
            if (String.IsNullOrEmpty(charged) || charged == " ")
                charged = "0";
            if (String.IsNullOrEmpty(recalc) || recalc == " ")
                recalc = "0";
            if (String.IsNullOrEmpty(innerBalance) || innerBalance == " ")
                innerBalance = "0";
            if (String.IsNullOrEmpty(changed) || changed == " ")
                changed = "0";
            if (String.IsNullOrEmpty(payment) || payment == " ")
                payment = "0";
            if (String.IsNullOrEmpty(chargedPayment) || chargedPayment == " ")
                chargedPayment = "0";
            if (String.IsNullOrEmpty(outerBalance) || outerBalance == " ")
                outerBalance = "0";

            tariff = tariff.Replace(',', '.');
            expense = expense.Replace(',', '.');
            underdelivery = underdelivery.Replace(',', '.');
            charged = charged.Replace(',', '.');
            recalc = recalc.Replace(',', '.');
            innerBalance = innerBalance.Replace(',', '.');
            changed = changed.Replace(',', '.');
            payment = payment.Replace(',', '.');
            chargedPayment = chargedPayment.Replace(',', '.');
            outerBalance = outerBalance.Replace(',', '.');

            string cmdText = @"UPDATE HCS_HOUSE_ACCOUNT_CHARGE 
                                SET TARIFF = " + tariff +
                                ", EXPENSE = " + expense +
                                ", UNDERDELIVERY = " + underdelivery +
                                ", CHARGED = " + charged +
                                ", RECALC = " + recalc +
                                ", INNER_BALANCE = " + innerBalance +
                                ", CHANGED = " + changed +
                                ", PAYMENT = " + payment +
                                ", CHARGED_PAYMENT = " + chargedPayment +
                                ", OUTER_BALANCE = " + outerBalance +
                                " WHERE ID = " + id;
            OracleConnection conn = new OracleConnection(connStr);
            OracleCommand cmd1 = new OracleCommand(cmdText, conn);
            conn.Open();
            try
            {
                cmd1.ExecuteNonQuery();
                return "ЗАГРУЖЕНО";
            }
            catch (Exception e)
            {
                return e.ToString();
            }
            finally
            {
                conn.Close();
            }
        }

        private String InsertHcsHouseAccountCharge(Int32 accountId, Int32 roId, String tariff, String expense, String underdelivery, String charged, String recalc,
            String innerBalance, String changed, String payment, String chargedPayment, String outerBalance, String service, String dateCharging, String supp)
        {
            string connStr = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=85.140.61.250)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ezhkh)));User Id = b4_gkh_samara; Password = ACTANONVERBA";

            if (String.IsNullOrEmpty(tariff) || tariff == " ")
                tariff = "0";
            if (String.IsNullOrEmpty(expense) || expense == " ")
                expense = "0";
            if (String.IsNullOrEmpty(underdelivery) || underdelivery == " ")
                underdelivery = "0";
            if (String.IsNullOrEmpty(charged) || charged == " ")
                charged = "0";
            if (String.IsNullOrEmpty(recalc) || recalc == " ")
                recalc = "0";
            if (String.IsNullOrEmpty(innerBalance) || innerBalance == " ")
                innerBalance = "0";
            if (String.IsNullOrEmpty(changed) || changed == " ")
                changed = "0";
            if (String.IsNullOrEmpty(payment) || payment == " ")
                payment = "0";
            if (String.IsNullOrEmpty(chargedPayment) || chargedPayment == " ")
                chargedPayment = "0";
            if (String.IsNullOrEmpty(outerBalance) || outerBalance == " ")
                outerBalance = "0";

            tariff = tariff.Replace(',', '.');
            expense = expense.Replace(',', '.');
            underdelivery = underdelivery.Replace(',', '.');
            charged = charged.Replace(',', '.');
            recalc = recalc.Replace(',', '.');
            innerBalance = innerBalance.Replace(',', '.');
            changed = changed.Replace(',', '.');
            payment = payment.Replace(',', '.');
            chargedPayment = chargedPayment.Replace(',', '.');
            outerBalance = outerBalance.Replace(',', '.');

            string cmdText = @"INSERT INTO HCS_HOUSE_ACCOUNT_CHARGE(ID, OBJECT_VERSION, OBJECT_CREATE_DATE, OBJECT_EDIT_DATE, SERVICE, TARIFF, EXPENSE, UNDERDELIVERY, 
								CHARGED, RECALC, CHANGED, PAYMENT, CHARGED_PAYMENT, OUTER_BALANCE, INNER_BALANCE, REALITY_OBJECT_ID, ACCOUNT_ID,
								DATE_CHARGING, SUPPLIER) 
								VALUES(HIBERNATE_SEQUENCE.NEXTVAL, 0, CURRENT_DATE, CURRENT_DATE, '" + service
                                + "'," + tariff + ", "
                                + expense + ", " + underdelivery
                                + ", " + charged + ", " + recalc
                                + ", " + changed + "," + payment
                                + ", " + chargedPayment + ", "
                                + outerBalance + ", " + innerBalance + ", " + roId + ", " + accountId +
                                ", to_date('" + dateCharging + "', 'dd.mm.yyyy'),'" + supp + "')";
            OracleConnection conn = new OracleConnection(connStr);
            OracleCommand cmd1 = new OracleCommand(cmdText, conn);         
            conn.Open();
            try
            {
                cmd1.ExecuteNonQuery();
                return "ЗАГРУЖЕНО";
            }
            catch (Exception e)
            {
                return e.ToString();
            }
            finally
            {
                conn.Close();
            }
        }

        private DataTable SelectLSInfoCounters(String database)
        {
            //string connStr = "Server=localhost;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
            string connStr = "Server=192.168.1.25;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
            NpgsqlConnection conn = new NpgsqlConnection(connStr);
      
            //NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
            //DataTable dt = new DataTable();
            conn.Open();
            try
            {
                string cmdText = @"drop table if exists t_couns_test";
                NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);               
                cmd.ExecuteNonQuery();

                cmdText = @"Create temp table t_couns_test(nzp_kvar integer, nzp_serv INTEGER, address char(100), nkvar char(10), pkod numeric(13,0), service char(30), 
                            num_cnt char(30), dat_uchet Date, dat_uchet_pred Date, val_cnt numeric(14,2), val_cnt_pred numeric(14,2), diff numeric(14,2))";
                cmd = new NpgsqlCommand(cmdText, conn);
                cmd.ExecuteNonQuery();

                cmdText = @"INSERT INTO t_couns_test
                            SELECT cs.nzp_kvar, cs.nzp_serv, ulica || ', д.' || ndom as address, nkvar, pkod, serv.service, cs1.num_cnt, max(dat_uchet) as dat_uchet, null, 0, 0 , 0
                            FROM bill01_data.counters cs
                            INNER JOIN bill01_data.counters_spis cs1 on cs1.nzp_counter = cs.nzp_counter
                            INNER JOIN fbill_data.kvar k on k.nzp_kvar = cs.nzp_kvar
                            INNER JOIN fbill_data.dom d on d.nzp_dom = k.nzp_dom 
                            INNER JOIN fbill_data.s_ulica u on u.nzp_ul = d.nzp_ul 
                            INNER JOIN fbill_kernel.services serv on serv.nzp_serv = cs.nzp_serv
                            where k.nzp_dom in (30883, 7154256) and cs.is_ac tual != 100
                            group by 1,2,3,4,5,6,7";
                cmd = new NpgsqlCommand(cmdText, conn);
                cmd.ExecuteNonQuery();

                cmdText = @"UPDATE t_couns_test SET val_cnt = 
                            (SELECT max(val_cnt) 
                            FROM bill01_data.counters c 
                            inner join bill01_data.counters_spis cs on cs.nzp_counter = c.nzp_counter 
                            where c.nzp_kvar = t_couns_test.nzp_kvar and c.nzp_serv = t_couns_test.nzp_serv and cs.num_cnt = t_couns_test.num_cnt and c.dat_uchet = t_couns_test.dat_uchet 
                                and is_actual != 100)";
                cmd = new NpgsqlCommand(cmdText, conn);
                cmd.ExecuteNonQuery();

                cmdText = @"UPDATE t_couns_test SET dat_uchet_pred = 
                            (SELECT max(dat_uchet) 
                            FROM bill01_data.counters c 
                            inner join bill01_data.counters_spis cs on cs.nzp_counter = c.nzp_counter 
                            where c.nzp_kvar = t_couns_test.nzp_kvar and c.nzp_serv = t_couns_test.nzp_serv and cs.num_cnt = t_couns_test.num_cnt and c.dat_uchet != t_couns_test.dat_uchet 
                                and is_actual != 100)";
                cmd = new NpgsqlCommand(cmdText, conn);
                cmd.ExecuteNonQuery();

                cmdText = @"UPDATE t_couns_test SET val_cnt_pred = 
                            (SELECT max(val_cnt) 
                            FROM bill01_data.counters c 
                            inner join bill01_data.counters_spis cs on cs.nzp_counter = c.nzp_counter 
                            where c.nzp_kvar = t_couns_test.nzp_kvar and c.nzp_serv = t_couns_test.nzp_serv and cs.num_cnt = t_couns_test.num_cnt 
                                and c.dat_uchet = t_couns_test.dat_uchet_pred and is_actual != 100)";
                cmd = new NpgsqlCommand(cmdText, conn);
                cmd.ExecuteNonQuery();

                cmdText = @"UPDATE t_couns_test SET diff = CASE WHEN coalesce(val_cnt_pred, 0) > 0 THEN coalesce(val_cnt, 0) - coalesce(val_cnt_pred, 0) ELSE 0 END";
                cmd = new NpgsqlCommand(cmdText, conn);
                cmd.ExecuteNonQuery();

                cmdText = @"SELECT address, nkvar, pkod, service, num_cnt, dat_uchet, dat_uchet_pred, val_cnt, val_cnt_pred, diff FROM t_couns_test order by 1,2";
                cmd = new NpgsqlCommand(cmdText, conn);
                NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
                DataTable dt = new DataTable();
                da.Fill(dt);
                return dt;
            }
            catch
            {
                return null;
            }
            finally
            {
                conn.Close();
            }
        }

        private String GetHcsHouseAccountCountersId(Int32 accountId, Int32 roId, String service, String meterSerial)
        {

            string connStr = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=85.140.61.250)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ezhkh)));User Id = b4_gkh_samara; Password = ACTANONVERBA";

            string cmdText = "SELECT ID FROM HCS_METER_READING " +
                            " where REALITY_OBJECT_ID = " + roId +
                            " and ACCOUNT_ID = " + accountId +
                            " and Service = '" + service +
                            "' and METER_SERIAL = '" + meterSerial;

            NpgsqlConnection conn = new NpgsqlConnection(connStr);
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);
            NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            try
            {
                da.Fill(dt);
                if (dt.Rows.Count == 1)
                    return "НАЙДЕНО|" + dt.Rows[0][0].ToString();
                else
                    return "НАЙДЕНО|0";
            }
            catch (Exception e)
            {
                return "ОШИБКА|" + e.ToString();
            }
        }

        private String UpdateHcsHouseAccountCounters(Int32 id, String currentReadDate, String prevReadDate, String currenteRead, String prevRead, String expense)
        {
            string connStr = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=85.140.61.250)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ezhkh)));User Id = b4_gkh_samara; Password = ACTANONVERBA";

            if (String.IsNullOrEmpty(currenteRead) || currenteRead == " ")
                currenteRead = "0";
            if (String.IsNullOrEmpty(prevRead) || prevRead == " ")
                prevRead = "0";
            if (String.IsNullOrEmpty(expense) || expense == " ")
                expense = "0";

            currenteRead = currenteRead.Replace(',', '.');
            prevRead = prevRead.Replace(',', '.');
            expense = expense.Replace(',', '.');

            string cmdText = @"UPDATE HCS_METER_READING 
                                SET CURRENT_READ_DATE = to_date('" + currentReadDate + "', 'dd.mm.yyyy'), " +
                                ", PREV_READ_DATE = to_date('" + prevReadDate + "', 'dd.mm.yyyy'), " +
                                ", CURRENTE_READ = " + currenteRead +
                                ", PREV_READ = " + prevRead +
                                ", EXPENSE = " + expense +
                                " WHERE ID = " + id;
            OracleConnection conn = new OracleConnection(connStr);
            OracleCommand cmd1 = new OracleCommand(cmdText, conn);
            conn.Open();
            try
            {
                cmd1.ExecuteNonQuery();
                return "ЗАГРУЖЕНО";
            }
            catch (Exception e)
            {
                return e.ToString();
            }
            finally
            {
                conn.Close();
            }
        }

        private String InsertHcsHouseAccountCounters(Int32 accountId, Int32 roId, String currentReadDate, String prevReadDate,
            String currenteRead, String prevRead, String expense, String service, String meterSerial)
        {
            string connStr = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=85.140.61.250)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ezhkh)));User Id = b4_gkh_samara; Password = ACTANONVERBA";

            if (String.IsNullOrEmpty(currenteRead) || currenteRead == " ")
                currenteRead = "0";
            if (String.IsNullOrEmpty(prevRead) || prevRead == " ")
                prevRead = "0";
            if (String.IsNullOrEmpty(expense) || expense == " ")
                expense = "0";

            currenteRead = currenteRead.Replace(',', '.');
            prevRead = prevRead.Replace(',', '.');
            expense = expense.Replace(',', '.');


            //to_date('" + dateCharging + "', 'dd.mm.yyyy'),

            string cmdText = @"INSERT INTO HCS_METER_READING(ID, OBJECT_VERSION, OBJECT_CREATE_DATE, OBJECT_EDIT_DATE, SERVICE, METER_SERIAL, CURRENT_READ_DATE, PREV_READ_DATE, 
								CURRENTE_READ, PREV_READ, EXPENSE, REALITY_OBJECT_ID, ACCOUNT_ID ) 
								VALUES(HIBERNATE_SEQUENCE.NEXTVAL, 0, CURRENT_DATE, CURRENT_DATE, '" + service
                                + "','" + meterSerial
                                 + "', to_date('" + currentReadDate + "', 'dd.mm.yyyy'), to_date('" + prevReadDate + "', 'dd.mm.yyyy'),"
                                + currenteRead + ", " + prevRead
                                + ", " + expense + "," + roId + ", " + accountId + ")";
            OracleConnection conn = new OracleConnection(connStr);
            OracleCommand cmd1 = new OracleCommand(cmdText, conn);
            conn.Open();
            try
            {
                cmd1.ExecuteNonQuery();
                return "ЗАГРУЖЕНО";
            }
            catch (Exception e)
            {
                return e.ToString();
            }
            finally
            {
                conn.Close();
            }
        }
    }
}
