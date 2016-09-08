using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.OracleClient;
using System.IO;
using Npgsql;
using XmlClass;
using System.Linq;
using System.Configuration;

namespace ServiceFromBill
{
    // ПРИМЕЧАНИЕ. Команду "Переименовать" в меню "Рефакторинг" можно использовать для одновременного изменения имени класса "Service1" в коде, SVC-файле и файле конфигурации.
    // ПРИМЕЧАНИЕ. Чтобы запустить клиент проверки WCF для тестирования службы, выберите элементы Service1.svc или Service1.svc.cs в обозревателе решений и начните отладку.
    public class Service1 : IService1
    {
        public List<BaseServ> ListKommServ;
        public BaseServ SummaryServ;
        public List<ServReval> ListReval;
        public List<Counters> ListCounters;
        public List<Counters> ListDomCounters;
        public CUnionServ CUnionServ;
        public List<BaseServ> ListServ;
        public List<ServVolume> ListVolume;
        public Dictionary<int, string> _formulList;
        public Decimal HvsNorm = 0;
        public Decimal GvsNorm = 0;
        public Decimal CalcSquare = 0;
        public Decimal FullSquare = 0;
        public Decimal DomSquare = 0;
        public Decimal MopSquare = 0;
        public String RashDpuPu = "0";
        public Boolean HasElDpu = false;
        public Boolean HasHvsDpu = false;
        public Boolean HasGvsDpu = false;
        public Boolean HasGazDpu = false;
        public Boolean HasOtopDpu = false;
        public String Kfodngvs;
        public String Kfodnhvs;
        public String KfodnEl;
        public Decimal KanNormCalc;
        public Decimal GvsNormGkal;
        public Boolean IsolateFlat;
        public Decimal OtopNorm;
        public Decimal LiveSquare;
        public string NumberDom;
        public string connStr;
        public NpgsqlConnection conn;

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
                case "radelit":
                    database = "RadElit";
                    break;
                case "kinel":
                    database = "kinel";
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

        public List<ЛицевойСчет> GetFactura(String db, Int32 m, Int32 y, Int32 part, Int32 numLs = 0)
        {
            switch (db.ToLower())
            {
                case "billauk":
                    db = "billAuk";
                    break;
                case "billtlt":
                    db = "billTlt";
                    break;
                case "radelit":
                    db = "RadElit";
                    break;
                case "kinel":
                    db = "kinel";
                    break;
                default:
                    db = "billTlt";
                    break;
            }
            string connStr = ConfigurationManager.ConnectionStrings[db].ConnectionString;
            //StreamWriter sw = new StreamWriter(@"C:\Temp\facturaLog.txt", true);
            conn = new NpgsqlConnection(connStr);
            conn.Open();
            List<ЛицевойСчет> returnData = new List<ЛицевойСчет>();
            var months = new[] {"","Январь","Февраль",
                 "Март","Апрель","Май","Июнь","Июль","Август","Сентябрь",
                 "Октябрь","Ноябрь","Декабрь"};
            String nzp_kvar = "0";
            String tempVal = "";
            try
            {
                
                String whereDom = "0";
                DropTable("fsel_kvar", db);
                CreateFselKvar(db);
                if (numLs == 0)
                {
                    if(db == "kinel")
                    {
                        whereDom = "SELECT nzp_dom FROM fbill_data.dom";
                    }
                    else
                    {
                        switch (part)
                        {
                            case 1:
                                {
                                    whereDom = "30913,30900,49,30902,48,47,30912,30901,46,30904,30903,30899,30895,30893,30898,30892,44,30737,30896,30888,30753,7155100,7155101,30905,30890,30889,30880,30878,30831,30881,51";
                                    break;
                                }
                            case 2:
                                {
                                    whereDom = "45,7154259,30988,30879,7155104,30685,230003,7155107";
                                    break;
                                }
                            case 3:
                                {
                                    whereDom = "7155108,30886,7155103,30883,7155106,7154254,52";
                                    break;
                                }
                            case 4:
                                {
                                    whereDom = "7155102,7154256,30978,30884,30897,230001";
                                    break;
                                }
                            case 5:
                                {
                                    whereDom = "30885,7154109,7154257,7155105,7154590";
                                    break;
                                }
                        }
                    }
                    
                    FillFselKvar(db, whereDom);
                }
                else
                {
                    FillFselKvar(db, numLs);
                }
                DropTable("t_fkvar_prm", db);
                CreateTFkvarPrm(db);
                FillTFkvarPrm(db);
                CreateIndex("t_fkvar_prm", db, "ix_fselkv_01", "nzp_kvar");
                Analyze("t_fkvar_prm", db);
                DropTable("t_freasonReval", db);
                CreateTFreasonReval(db);
                DropTable("t_fVolume", db);
                CreateTFVolume(db);
                DropTable("t_fPerekidka", db);
                CreateTFPerekidka(db);
                UpdateTFkvarPrm(db, m, y, 1);
                DropTable("t_12", db);
                CreateT12(db);
                FillT12(db, m, y);
                UpdateTFkvarPrm(db, m, y, 2);
                DropTable("t_12", db);
                UpdateTFkvarPrm(db, m, y, 3);
                FillTFreasonReval(db, m, y);
                FillTFPerekidka(db, m, y);
                FillTFVolume(db, m, y);
                UpdateFselKvar(db);
                CreateIndex("t_freasonReval", db, "ix_fselkv_03", "nzp_kvar, nzp_serv");
                Analyze("t_freasonReval", db);
                CreateIndex("t_fVolume", db, "ix_fselkv_04", "nzp_kvar, nzp_serv");
                Analyze("t_fVolume", db);
                CreateIndex("t_fPerekidka", db, "ix_fselkv_06", "nzp_kvar, nzp_serv");
                Analyze("t_fPerekidka", db);
                DataTable part1 = SelectPart1(db);

                for (int i = 0; i < part1.Rows.Count; i++)
                {
                    ListReval = new List<ServReval>();
                    CUnionServ = new CUnionServ();
                    FillCunionServ(db);
                    ListKommServ = new List<BaseServ>();
                    SummaryServ = new BaseServ(false);
                    ListServ = new List<BaseServ>();
                    ListVolume = new List<ServVolume>();
                    ListDomCounters = new List<Counters>();
                    ListCounters = new List<Counters>();
                    GetListKommServ(db);
                    ЛицевойСчет лс = new ЛицевойСчет();
                    Раздел1 раздел1 = new Раздел1();
                    раздел1.Период = months[m] + " " + y + "г.";
                    //Квартирные параметры
                    nzp_kvar = part1.Rows[i]["nzp_kvar"].ToString();
                    //sw.WriteLine(nzp_kvar);
                    DataTable kvarPrm = SelectFKvarPrm(db, part1.Rows[i]["nzp_kvar"].ToString());
                    раздел1.Приватизирована = kvarPrm.Rows[0]["privat"] != null && kvarPrm.Rows[0]["privat"].ToString() != "" ? "Да" : "Нет";
                    раздел1.НомерЛС = part1.Rows[i]["num_ls"].ToString();
                    раздел1.ФИО = part1.Rows[i]["fio"].ToString();
                    if(db == "kinel")
                    {
                        раздел1.АдресПомещения = kvarPrm.Rows[0]["indecs"].ToString() + ", " + part1.Rows[i]["town"].ToString() + ", " 
                            + part1.Rows[i]["rajon"].ToString() + ", " + part1.Rows[i]["ulica"].ToString() + "," 
                            + part1.Rows[i]["ndom"].ToString() + "-" + part1.Rows[i]["nkvar"].ToString();
                    }
                    else
                    {
                        раздел1.АдресПомещения = kvarPrm.Rows[0]["indecs"].ToString() + ", г. Самара, "
                        + part1.Rows[i]["ulica"].ToString() + "," + part1.Rows[i]["ndom"].ToString() + "-" + part1.Rows[i]["nkvar"].ToString();
                    }
                    
                    
                    раздел1.ПрописаноПроживает = kvarPrm.Rows[0]["count_gil"].ToString() != ""
                        ? kvarPrm.Rows[0]["count_gil"].ToString() : "0" + "/" + kvarPrm.Rows[0]["count_gilp"].ToString() != ""
                            ? kvarPrm.Rows[0]["count_gilp"].ToString() : "0";
                    try
                    {
                        раздел1.ПлощадьДома = kvarPrm.Rows[0]["pl_dom"] != null && kvarPrm.Rows[0]["pl_dom"].ToString() != ""
                        ? Convert.ToDecimal(kvarPrm.Rows[0]["pl_dom"]).ToString("0.00") : "0.00";
                    }
                    catch
                    {
                        раздел1.ПлощадьДома = kvarPrm.Rows[0]["pl_dom"] != null && kvarPrm.Rows[0]["pl_dom"].ToString() != ""
                        ? Convert.ToDecimal(kvarPrm.Rows[0]["pl_dom"].ToString().Replace(",", ".")).ToString("0.00") : "0.00";
                    }

                    try
                    {
                        раздел1.ПлощадьМОП = kvarPrm.Rows[0]["pl_mop"] != null && kvarPrm.Rows[0]["pl_mop"].ToString() != ""
                        ? Convert.ToDecimal(kvarPrm.Rows[0]["pl_mop"].ToString()).ToString("0.00") : "0.00";
                    }
                    catch
                    {
                        раздел1.ПлощадьМОП = kvarPrm.Rows[0]["pl_mop"] != null && kvarPrm.Rows[0]["pl_mop"].ToString() != ""
                        ? Convert.ToDecimal(kvarPrm.Rows[0]["pl_mop"].ToString().Replace(",", ".")).ToString("0.00") : "0.00";
                    }
                    //sw.WriteLine("1");
                    раздел1.Проживает = kvarPrm.Rows[0]["count_domgil"].ToString() != "" ? kvarPrm.Rows[0]["count_domgil"].ToString() : "0";
                    DataTable orgInfo = SelectOrgInfo(db);
                    раздел1.Организация = orgInfo.Rows[0]["sb10"].ToString() + ", фактич. адрес " + orgInfo.Rows[0]["sb17"].ToString() + ", тел." +
                        orgInfo.Rows[0]["sb16"].ToString() + "; р/с " + orgInfo.Rows[0]["sb12"].ToString() + " в " + orgInfo.Rows[0]["sb11"].ToString();
                    лс.Раздел1 = раздел1;
                    NumberDom = part1.Rows[i]["ndom"].ToString();
                    Раздел2 раздел2 = new Раздел2();
                    раздел2.ПолучательПлатежа = orgInfo.Rows[0]["sb1"].ToString() + " ИНН-" + orgInfo.Rows[0]["sb6"].ToString();
                    раздел2.БанковскийСчет = "Р/с - " + orgInfo.Rows[0]["sb3"].ToString() + "   Кор/счет-" + orgInfo.Rows[0]["sb4"].ToString() + "  " +
                            "  БИК " + orgInfo.Rows[0]["sb5"].ToString() + " " + orgInfo.Rows[0]["sb2"].ToString();
                    раздел2.ПлатежныйКод = part1.Rows[i]["pkod"].ToString();
                    раздел2.ВидПлаты = "кв/плата и к/услуги";
                    раздел2.СуммаКОплате = "";//ДОДЕЛАТЬ
                    лс.Раздел2 = раздел2;
                    //sw.WriteLine("1_1");
                    DataTable prm2074 = SelectPrm2074(db, part1.Rows[i]["nzp_kvar"].ToString());
                    //sw.WriteLine("1_1_1");
                    Decimal valPrm2074 = 0;
                    try
                    {
                        //sw.WriteLine(prm2074.Rows[0]["val_prm"].ToString());
                        valPrm2074 = prm2074.Rows.Count > 0 ? Convert.ToDecimal(prm2074.Rows[0]["val_prm"]) : 0;
                    }
                    catch
                    {
                        //sw.WriteLine("1_1_2");
                        //sw.WriteLine(prm2074.Rows[0]["val_prm"].ToString().Replace(",", "."));
                        valPrm2074 = prm2074.Rows.Count > 0 ? Convert.ToDecimal(prm2074.Rows[0]["val_prm"].ToString().Replace(".", ",")) : 0;
                    }
                    //sw.WriteLine("1_2");
                    FillServise(db, m, y, part1.Rows[i]["nzp_kvar"].ToString());
                    FillInfo(db, m, y, part1.Rows[i]["nzp_kvar"].ToString());
                    FillDomServise(db, m, y, part1.Rows[i]["nzp_kvar"].ToString(), part1.Rows[i]["nzp_dom"].ToString());
                    FillServNorm(db, m, y, part1.Rows[i]["nzp_kvar"].ToString());
                    FillDomCounters(db, m, y, part1.Rows[i]["nzp_kvar"].ToString(), part1.Rows[i]["nzp_dom"].ToString());
                    FillCounters(db, m, y, part1.Rows[i]["nzp_kvar"].ToString(), part1.Rows[i]["nzp_dom"].ToString());

                    try
                    {
                        GvsNormGkal = kvarPrm.Rows[0]["gvs_norm_gkal"].ToString() != "" ? Convert.ToDecimal(kvarPrm.Rows[0]["gvs_norm_gkal"]) : 0;
                    }
                    catch
                    {
                        GvsNormGkal = kvarPrm.Rows[0]["gvs_norm_gkal"].ToString() != "" ? Convert.ToDecimal(kvarPrm.Rows[0]["gvs_norm_gkal"].ToString().Replace(",", ".")) : 0;
                    }
                    //sw.WriteLine("1_3");
                    GvsNormGkal = GvsNormGkal == 0 ? 0.0611m : GvsNormGkal;

                    IsolateFlat = true;
                    if (kvarPrm.Rows[0]["is_komm"] != DBNull.Value)
                        IsolateFlat = Convert.ToInt32(kvarPrm.Rows[0]["is_komm"]) == 0;
                    if (IsolateFlat)
                    {
                        try
                        {
                            if (kvarPrm.Rows[0]["otop_norm_i"] != DBNull.Value)
                                OtopNorm = Convert.ToDecimal(kvarPrm.Rows[0]["otop_norm_i"]);
                            раздел1.ПлощадьПомещения = kvarPrm.Rows[0]["pl_kvar"] != null && kvarPrm.Rows[0]["pl_kvar"].ToString() != ""
                            ? Convert.ToDecimal(kvarPrm.Rows[0]["pl_kvar"]).ToString("0.00") : "0";
                        }
                        catch
                        {
                            if (kvarPrm.Rows[0]["otop_norm_i"] != DBNull.Value)
                                OtopNorm = Convert.ToDecimal(kvarPrm.Rows[0]["otop_norm_i"].ToString().Replace(",", "."));
                            раздел1.ПлощадьПомещения = kvarPrm.Rows[0]["pl_kvar"] != null && kvarPrm.Rows[0]["pl_kvar"].ToString() != ""
                            ? Convert.ToDecimal(kvarPrm.Rows[0]["pl_kvar"].ToString().Replace(",", ".")).ToString("0.00") : "0";
                        }
                        
                    }
                    else
                    {
                        try
                        {
                            if (kvarPrm.Rows[0]["otop_norm_k"] != DBNull.Value)
                                OtopNorm = Convert.ToDecimal(kvarPrm.Rows[0]["otop_norm_k"]);
                            раздел1.ПлощадьПомещения = kvarPrm.Rows[0]["pl_kvar_gil"] != null && kvarPrm.Rows[0]["pl_kvar_gil"].ToString() != ""
                            ? Convert.ToDecimal(kvarPrm.Rows[0]["pl_kvar_gil"]).ToString("0.00") : "0";
                        }
                        catch
                        {
                            if (kvarPrm.Rows[0]["otop_norm_k"] != DBNull.Value)
                                OtopNorm = Convert.ToDecimal(kvarPrm.Rows[0]["otop_norm_k"].ToString().Replace(",", "."));
                            раздел1.ПлощадьПомещения = kvarPrm.Rows[0]["pl_kvar_gil"] != null && kvarPrm.Rows[0]["pl_kvar_gil"].ToString() != ""
                            ? Convert.ToDecimal(kvarPrm.Rows[0]["pl_kvar_gil"].ToString().Replace(",", ".")).ToString("0.00") : "0";
                        }
                        
                    }
                    //sw.WriteLine("1_4");
                    try
                    {
                        LiveSquare = kvarPrm.Rows[0]["pl_kvar_gil"] != null && kvarPrm.Rows[0]["pl_kvar_gil"].ToString() != ""
                        ? Convert.ToDecimal(kvarPrm.Rows[0]["pl_kvar_gil"]) : 0;
                        DomSquare = kvarPrm.Rows[0]["pl_dom"] != null && kvarPrm.Rows[0]["pl_dom"].ToString() != ""
                            ? Convert.ToDecimal(kvarPrm.Rows[0]["pl_dom"]) : 0;
                        MopSquare = kvarPrm.Rows[0]["pl_mop"] != null && kvarPrm.Rows[0]["pl_mop"].ToString() != ""
                            ? Convert.ToDecimal(kvarPrm.Rows[0]["pl_mop"]) : 0;
                    }
                    catch
                    {
                        LiveSquare = kvarPrm.Rows[0]["pl_kvar_gil"] != null && kvarPrm.Rows[0]["pl_kvar_gil"].ToString() != ""
                        ? Convert.ToDecimal(kvarPrm.Rows[0]["pl_kvar_gil"].ToString().Replace(",", ".")) : 0;
                        DomSquare = kvarPrm.Rows[0]["pl_dom"] != null && kvarPrm.Rows[0]["pl_dom"].ToString() != ""
                            ? Convert.ToDecimal(kvarPrm.Rows[0]["pl_dom"].ToString().Replace(",", ".")) : 0;
                        MopSquare = kvarPrm.Rows[0]["pl_mop"] != null && kvarPrm.Rows[0]["pl_mop"].ToString() != ""
                            ? Convert.ToDecimal(kvarPrm.Rows[0]["pl_mop"].ToString().Replace(",", ".")) : 0;
                    }
                    //sw.WriteLine("2");

                    GetRashDpuPu(db, part1.Rows[i]["nzp_kvar"].ToString());

                    Раздел3 раздел3 = new Раздел3();
                    List<XmlClass.Услуга> услуги = new List<XmlClass.Услуга>();
                    Раздел4 раздел4 = new Раздел4();
                    List<СправочнаяИнформация> справочныеДанные = new List<СправочнаяИнформация>();

                    foreach (BaseServ aServ in ListServ)
                    {
                        if (aServ.Serv.NameServ.Trim().Contains("п\\к") &&
                            aServ.Serv.NameServ.Trim().Contains("ОДН-Горячая вода"))
                        {
                            foreach (BaseServ aServMain in this.ListServ)
                            {
                                if (!aServMain.Serv.NameServ.Trim().Contains("п\\к") &&
                                    aServMain.Serv.NameServ.Trim().Contains("Горячая вода"))
                                {
                                    aServMain.ServOdn.RsumTarif += aServ.Serv.RsumTarif;
                                    aServMain.Serv.RsumTarif += aServ.Serv.RsumTarif;
                                }
                            }
                        }
                        else if (aServ.Serv.NameServ.Trim().Contains("п\\к") &&
                            aServ.Serv.NameServ.Trim().Contains("ОДН-Холодная вода для нужд ГВС"))
                        {
                            foreach (BaseServ aServMain in ListServ)
                            {
                                if (!aServMain.Serv.NameServ.Trim().Contains("п\\к") &&
                                    aServMain.Serv.NameServ.Trim().Contains("для ГВС"))
                                {
                                    aServMain.ServOdn.RsumTarif += aServ.Serv.RsumTarif;
                                    aServMain.Serv.RsumTarif += aServ.Serv.RsumTarif;
                                }
                            }
                        }
                    }
                    //sw.WriteLine("3");
                    SetServRashod();
                    ListServ.Sort();
                    ListServ = SortServ(ListServ);
                    Decimal d1 = 0;

                    foreach (BaseServ aServ in this.ListServ)
                    {
                        if (IsShowServInGrid(aServ))
                        {
                            XmlClass.Услуга услуга = new XmlClass.Услуга();
                            СправочнаяИнформация справочнаяИнформация = new СправочнаяИнформация();
                            справочнаяИнформация.ВидУслуги = "";
                            справочнаяИнформация.НормативПотребления = new НормативПотребления();
                            справочнаяИнформация.ОбъемКоммунальныхУслуг4 = new ОбъемКоммунальныхУслуг4();
                            справочнаяИнформация.Показания = new Показания();
                            if (aServ.Serv.Tarif == 0m)
                                continue;
                            string servName;
                            try
                            {
                                servName = aServ.Serv.NameSupp.Trim().Split(',')[1].Trim();
                            }
                            catch
                            {
                                try
                                {
                                    servName = aServ.Serv.NameSupp.Trim().Split('/')[1].Trim();
                                }
                                catch
                                {
                                    servName = aServ.Serv.NameSupp.Trim();
                                }
                            }

                            if (servName.Length == 0)
                                услуга.ВидУслуги = aServ.Serv.NameServ.Trim();
                            else
                                услуга.ВидУслуги = aServ.Serv.NameServ.Trim() + "-" + servName;
                            if ((aServ.Serv.NameServ.Trim().Contains("п\\к") &&
                             aServ.Serv.NameServ.Trim().Contains("ОДН-Горячая вода")) ||
                            (aServ.Serv.NameServ.Trim().Contains("п\\к") &&
                             aServ.Serv.NameServ.Trim().Contains("ОДН-Холодная вода для нужд ГВС")))
                            {

                            }
                            else
                            {
                                if (aServ.Serv.NameServ.Trim() == "Электроснабжение" && (aServ.Serv.Tarif == 2.45m || aServ.Serv.Tarif == 7.92m))
                                {
                                    услуга.ВидУслуги = "ОДН-Электроснабжение день";
                                    услуга.ЕдиницаИзмерения = aServ.Serv.Measure.Trim();
                                    ОбъемКоммунальныхУслуг объемКоммунальныхУслуг = new ОбъемКоммунальныхУслуг();
                                    if (Math.Abs(aServ.ServOdn.CCalc) > 0.00001m)
                                    {
                                        string str2 = "(1)";
                                        if (aServ.Serv.NzpServ == 6 & HasHvsDpu)
                                            str2 = "(4)";
                                        if (aServ.Serv.NzpServ == 9 & HasGvsDpu)
                                            str2 = "(4)";
                                        if (aServ.Serv.NzpServ == 14 & HasGvsDpu)
                                            str2 = "(4)";
                                        if (aServ.Serv.NzpServ == 25 & HasElDpu)
                                            str2 = "(4)";
                                        объемКоммунальныхУслуг.ОбщедомовыеНужды = (aServ.ServOdn.CCalc.ToString("0.0000") + str2);
                                    }
                                    if (aServ.Serv.NzpServ == 7 && aServ.Serv.NzpFrm == 26907209)
                                    {
                                        foreach (ServVolume servVolume in ListVolume)
                                        {
                                            if (servVolume.NzpServ == 7)
                                                servVolume.NormaVolume = KanNormCalc;
                                        }
                                    }
                                    услуга.Тариф = "2.45";
                                    РазмерПлатыЗаКоммунальныеУслуги размерПлатыЗаКоммунальныеУслуги = new РазмерПлатыЗаКоммунальныеУслуги();
                                    if (Math.Abs(aServ.Serv.RsumTarif - aServ.ServOdn.RsumTarif) > 0.001m)
                                        размерПлатыЗаКоммунальныеУслуги.ИндивидуальноеПотребление = "";
                                    if (Math.Abs(aServ.ServOdn.RsumTarif) > 0.001m)
                                        размерПлатыЗаКоммунальныеУслуги.ОбщедомовыеНужды = aServ.ServOdn.RsumTarif.ToString("0.00");
                                    услуга.ВсегоНачислено = aServ.ServOdn.RsumTarif.ToString("0.00");
                                    услуга.Перерасчеты = "";
                                    услуга.РазмерПлатыЗаКоммунальныеУслуги = размерПлатыЗаКоммунальныеУслуги;
                                    if (Math.Abs(aServ.Serv.Reval + aServ.Serv.RealCharge) > Math.Abs(aServ.Serv.RsumTarif))
                                        d1 += aServ.Serv.Reval + aServ.Serv.RealCharge + aServ.Serv.RsumTarif;
                                    услуга.Льготы = "";
                                    // if (Math.Abs(aServ.Serv.SumCharge) > new Decimal(1, 0, 0, false, (byte)3))
                                    //   dr["sum_charge_all" + (object)index1] = (object)aServ.Serv.SumCharge.ToString("0.00");
                                    ИтогоКОплате итогоКОплате = new ИтогоКОплате();
                                    ЗаКоммульныеУслуги заКоммунальныеУслуги = new ЗаКоммульныеУслуги();
                                    if (Math.Abs(aServ.Serv.SumCharge - aServ.ServOdn.SumCharge) > new Decimal(1, 0, 0, false, (byte)3))
                                    {
                                        заКоммунальныеУслуги.ИндивидуальноеПотребление = (aServ.Serv.SumCharge - aServ.ServOdn.SumCharge).ToString("0.00");
                                    }



                                    if (Math.Abs(aServ.ServOdn.SumCharge) > 0.001m)
                                    {
                                        заКоммунальныеУслуги.ОбщедомовыеНужды = aServ.ServOdn.RsumTarif.ToString("0.00");
                                    }
                                    итогоКОплате.ЗаКоммульныеУслуги = заКоммунальныеУслуги;
                                    if (aServ.Serv.NzpMeasure == 4 & Math.Abs(aServ.Serv.RsumTarif) > new Decimal(1, 0, 0, false, (byte)3))
                                    {
                                        if (aServ.Serv.OldMeasure == 4)
                                        {
                                            if (aServ.Serv.NzpServ == 9)
                                            {
                                                if (Math.Abs(aServ.Serv.CCalc) > new Decimal(1, 0, 0, false, (byte)5))
                                                    объемКоммунальныхУслуг.ИндивидуальноеПотребление = (aServ.Serv.CCalc.ToString("0.0000") + GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice));
                                            }
                                            else
                                                объемКоммунальныхУслуг.ИндивидуальноеПотребление = (aServ.Serv.CCalc.ToString("0.0000") + GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice));
                                        }
                                        else if (aServ.Serv.NzpServ == 9)
                                        {
                                            if (Math.Abs(aServ.Serv.CCalc) > new Decimal(1, 0, 0, false, (byte)5))
                                            {
                                                объемКоммунальныхУслуг.ИндивидуальноеПотребление = (aServ.Serv.CCalc * GvsNormGkal).ToString("0.0000") +
                                                    GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice);
                                            }
                                        }
                                        else
                                        {
                                            объемКоммунальныхУслуг.ИндивидуальноеПотребление = (aServ.Serv.CCalc * OtopNorm).ToString("0.0000") +
                                                GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice);
                                        }
                                        if (Math.Abs(aServ.ServOdn.CCalc) > new Decimal(1, 0, 0, false, (byte)6))
                                        {
                                            string str2 = "(1)";
                                            if (HasGvsDpu)
                                                str2 = "(4)";
                                            if (aServ.Serv.NzpServ == 9)
                                                объемКоммунальныхУслуг.ОбщедомовыеНужды = (aServ.ServOdn.CCalc.ToString("0.0000") + str2);
                                            else
                                                объемКоммунальныхУслуг.ОбщедомовыеНужды = (aServ.ServOdn.CCalc.ToString("0.0000") + str2);
                                        }
                                        справочнаяИнформация =
                                            FillGoodServVolume(справочнаяИнформация, aServ.Serv.NzpServ == 9 ? this.GvsNormGkal : this.OtopNorm, "rash_norm");
                                    }
                                    услуга.ИтогоКОплате = итогоКОплате;
                                    услуга.ОбъемКоммунальныхУслуг = объемКоммунальныхУслуг;
                                    услуги.Add(услуга);

                                    try
                                    {
                                        справочнаяИнформация = FillServiceVolume(справочнаяИнформация, aServ.Serv.NzpServ, aServ.Serv.NameServ.Trim(),
                                                услуга.ОбъемКоммунальныхУслуг.ИндивидуальноеПотребление != "" ?
                                                Convert.ToDecimal(услуга.ОбъемКоммунальныхУслуг.ИндивидуальноеПотребление.Split('(')[0]) : 0);
                                    }
                                    catch (Exception ex)
                                    {
                                        try
                                        {
                                            справочнаяИнформация = FillServiceVolume(справочнаяИнформация, aServ.Serv.NzpServ, aServ.Serv.NameServ.Trim(),
                                                услуга.ОбъемКоммунальныхУслуг.ИндивидуальноеПотребление != "" ?
                                                Convert.ToDecimal(услуга.ОбъемКоммунальныхУслуг.ИндивидуальноеПотребление.Split('(')[0].Replace(",", ".")) : 0);
                                        }
                                        catch
                                        {

                                        }
                                    }
                                    справочнаяИнформация.ВидУслуги = услуга.ВидУслуги;
                                    справочныеДанные.Add(справочнаяИнформация);
                                    услуга = new XmlClass.Услуга();
                                    справочнаяИнформация = new СправочнаяИнформация();
                                    справочнаяИнформация.ВидУслуги = "";
                                    справочнаяИнформация.НормативПотребления = new НормативПотребления();
                                    справочнаяИнформация.ОбъемКоммунальныхУслуг4 = new ОбъемКоммунальныхУслуг4();
                                    справочнаяИнформация.Показания = new Показания();
                                    услуга.ВидУслуги = "Электроснабжение";
                                    услуга.ЕдиницаИзмерения = "кВт*час";
                                    итогоКОплате = new ИтогоКОплате();
                                    услуга.ИтогоКОплате = итогоКОплате;
                                    объемКоммунальныхУслуг = new ОбъемКоммунальныхУслуг();
                                    if (Math.Abs(aServ.Serv.CCalc) > 0.00001m & !aServ.Serv.IsOdn &&
                                        !(aServ.Serv.RsumTarif == aServ.ServOdn.RsumTarif & aServ.Serv.RsumTarif > 0.001m))
                                    {
                                        if (Math.Abs(aServ.Serv.RsumTarif) > 0.001m)
                                            объемКоммунальныхУслуг.ИндивидуальноеПотребление = (aServ.Serv.CCalc.ToString("0.00##") +
                                                GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice));
                                        else if (Math.Abs(aServ.ServOdn.CCalc) > 0.00001m)
                                            объемКоммунальныхУслуг.ИндивидуальноеПотребление = (aServ.Serv.CCalc.ToString("0.00##") +
                                                GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice));
                                    }
                                    if (Math.Abs(aServ.ServOdn.CCalc) > 0.00001m)
                                    {
                                        string str2 = "(1)";
                                        if (aServ.Serv.NzpServ == 6 & HasHvsDpu)
                                            str2 = "(4)";
                                        if (aServ.Serv.NzpServ == 9 & HasGvsDpu)
                                            str2 = "(4)";
                                        if (aServ.Serv.NzpServ == 14 & HasGvsDpu)
                                            str2 = "(4)";
                                        if (aServ.Serv.NzpServ == 25 & HasElDpu)
                                            str2 = "(4)";
                                    }
                                    if (aServ.Serv.NzpServ == 7 && aServ.Serv.NzpFrm == 26907209)
                                    {
                                        foreach (ServVolume servVolume in this.ListVolume)
                                        {
                                            if (servVolume.NzpServ == 7)
                                                servVolume.NormaVolume = this.KanNormCalc;
                                        }
                                    }
                                    if (Math.Abs(aServ.Serv.Tarif) > 0.0001m)
                                        услуга.Тариф = aServ.Serv.Tarif > 2.45m ? aServ.Serv.Tarif.ToString("00") : "2.41m";
                                    if (((aServ.Serv.NzpServ == 6 ? 1 : (aServ.Serv.NzpServ == 7 ? 1 : 0)) & (aServ.Serv.NzpMeasure != 3 ? 1 : 0)) != 0)
                                    {
                                        if (aServ.Serv.Norma > new Decimal(1, 0, 0, false, (byte)3))
                                        {
                                            услуга.Тариф = (aServ.Serv.Tarif / aServ.Serv.Norma).ToString("0.000");
                                        }
                                        услуга.ЕдиницаИзмерения = "Куб.м.";
                                    }
                                    размерПлатыЗаКоммунальныеУслуги = new РазмерПлатыЗаКоммунальныеУслуги();
                                    if (Math.Abs(aServ.Serv.RsumTarif) > 0.0001m)
                                    {
                                        размерПлатыЗаКоммунальныеУслуги.ИндивидуальноеПотребление = (aServ.Serv.RsumTarif - aServ.ServOdn.RsumTarif).ToString("0.00");
                                    }
                                    if (Math.Abs(aServ.Serv.RsumTarif) > new Decimal(1, 0, 0, false, (byte)3))
                                    {
                                        услуга.ВсегоНачислено = (aServ.Serv.RsumTarif - aServ.ServOdn.RsumTarif).ToString("00");
                                    }
                                    if (Math.Abs(aServ.Serv.Reval + aServ.Serv.RealCharge) > 0.001m)
                                    {
                                        услуга.Перерасчеты = (aServ.Serv.Reval + aServ.Serv.RealCharge).ToString("0.00");
                                    }
                                    /*if (Math.Abs(aServ.Serv.Reval + aServ.Serv.RealCharge) > Math.Abs(aServ.Serv.RsumTarif))
                                        num1 += aServ.Serv.Reval + aServ.Serv.RealCharge + aServ.Serv.RsumTarif;*/
                                    услуга.Льготы = "";
                                    услуга.РазмерПлатыЗаКоммунальныеУслуги = размерПлатыЗаКоммунальныеУслуги;
                                    //if (Math.Abs(aServ.Serv.SumCharge) > new Decimal(1, 0, 0, false, (byte)3))
                                    //   dr["sum_charge_all" + (object)index1] = (object)aServ.Serv.SumCharge.ToString("0.00");
                                    /*if (Math.Abs(aServ.Serv.SumCharge - aServ.ServOdn.SumCharge) > new Decimal(1, 0, 0, false, (byte)3))
                                    {
                                        DataRow dataRow = dr;
                                        string index2 = "sum_charge" + (object)index1;
                                        num2 = aServ.Serv.SumCharge - aServ.ServOdn.SumCharge;
                                        string str2 = num2.ToString("0.00");
                                        dataRow[index2] = (object)str2;
                                    }*/
                                    if (aServ.Serv.NzpMeasure == 4 & Math.Abs(aServ.Serv.RsumTarif) > 0.001m)
                                    {
                                        if (aServ.Serv.OldMeasure == 4)
                                        {
                                            if (aServ.Serv.NzpServ == 9)
                                            {
                                                if (Math.Abs(aServ.Serv.CCalc) > new Decimal(1, 0, 0, false, (byte)5))
                                                    объемКоммунальныхУслуг.ИндивидуальноеПотребление = (aServ.Serv.CCalc.ToString("0.0000") +
                                                        GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice));
                                            }
                                            else
                                                объемКоммунальныхУслуг.ИндивидуальноеПотребление = (aServ.Serv.CCalc.ToString("0.0000") +
                                                    GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice));
                                        }
                                        else if (aServ.Serv.NzpServ == 9)
                                        {
                                            if (Math.Abs(aServ.Serv.CCalc) > new Decimal(1, 0, 0, false, (byte)5))
                                                объемКоммунальныхУслуг.ИндивидуальноеПотребление = ((aServ.Serv.CCalc * GvsNormGkal).ToString("0.0000") +
                                                    GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice));
                                        }
                                        else
                                            объемКоммунальныхУслуг.ИндивидуальноеПотребление = ((aServ.Serv.CCalc * OtopNorm).ToString("0.0000") +
                                                GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice));
                                        if (Math.Abs(aServ.ServOdn.CCalc) > 0.000001m)
                                        {
                                            string str2 = "(1)";
                                            if (HasGvsDpu)
                                                str2 = "(4)";
                                            if (aServ.Serv.NzpServ == 9)
                                                объемКоммунальныхУслуг.ОбщедомовыеНужды = (aServ.ServOdn.CCalc.ToString("0.0000") + str2);
                                            else
                                                объемКоммунальныхУслуг.ОбщедомовыеНужды = (aServ.ServOdn.CCalc.ToString("0.0000") + str2);
                                        }
                                        справочнаяИнформация =
                                            FillGoodServVolume(справочнаяИнформация, aServ.Serv.NzpServ == 9 ? this.GvsNormGkal : this.OtopNorm, "rash_norm");
                                    }
                                    услуга.ОбъемКоммунальныхУслуг = объемКоммунальныхУслуг;
                                    услуги.Add(услуга);
                                    справочнаяИнформация.ВидУслуги = услуга.ВидУслуги;
                                    справочныеДанные.Add(справочнаяИнформация);
                                }
                                else if (aServ.Serv.NameServ.Trim() == "Электроснабжение ночное")
                                    услуга.ВидУслуги = "ОДН-Электроснабжение ночь";
                                else if (servName.Length == 0)
                                    услуга.ВидУслуги = aServ.Serv.NameServ.Trim();
                                else
                                    услуга.ВидУслуги = aServ.Serv.NameServ.Trim() + "-" + servName;
                                услуга.ЕдиницаИзмерения = aServ.Serv.Measure.Trim();
                                if (!(aServ.Serv.NameServ.Trim() == "Электроснабжение") || !(aServ.Serv.Tarif == 2.45m || aServ.Serv.Tarif == 7.92m))
                                {
                                    Decimal num2 = new Decimal(0);
                                    Decimal num3 = new Decimal(0);
                                    Decimal num4 = new Decimal(0);
                                    ОбъемКоммунальныхУслуг объемКоммунальныхУслуг = new ОбъемКоммунальныхУслуг();
                                    if (Math.Abs(aServ.Serv.CCalc) > 0.00001m & !aServ.Serv.IsOdn &&
                                        !(aServ.Serv.RsumTarif == aServ.ServOdn.RsumTarif & aServ.Serv.RsumTarif > 0.001m))
                                    {
                                        if (Math.Abs(aServ.Serv.RsumTarif) > new Decimal(1, 0, 0, false, (byte)3) && aServ.Serv.Tarif > 0.001m)
                                        {
                                            объемКоммунальныхУслуг.ИндивидуальноеПотребление = (aServ.Serv.CCalc.ToString("0.00##") +
                                                GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice));
                                            try
                                            {
                                                try
                                                {
                                                    num2 = aServ.Serv.CCalc + Convert.ToDecimal(GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice));
                                                }
                                                catch
                                                {
                                                    num2 = aServ.Serv.CCalc + Convert.ToDecimal(GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice).Replace(",", "."));
                                                }
                                                
                                            }
                                            catch (Exception ex1)
                                            {
                                                try
                                                {
                                                    num2 = aServ.Serv.CCalc;
                                                }
                                                catch (Exception ex2)
                                                {
                                                    num2 = new Decimal(0);
                                                }
                                            }
                                        }
                                        else if (Math.Abs(aServ.ServOdn.CCalc) > 0.00001m && aServ.Serv.Tarif > 0.001m)
                                        {
                                            объемКоммунальныхУслуг.ИндивидуальноеПотребление = (aServ.Serv.CCalc.ToString("0.00##") +
                                                GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice));
                                            try
                                            {
                                                try
                                                {
                                                    num2 = aServ.Serv.CCalc + Convert.ToDecimal(this.GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice));
                                                }
                                                catch
                                                {
                                                    num2 = aServ.Serv.CCalc + Convert.ToDecimal(this.GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice).Replace(",","."));
                                                }
                                               
                                            }
                                            catch (Exception ex1)
                                            {
                                                try
                                                {
                                                    num2 = aServ.Serv.CCalc;
                                                }
                                                catch (Exception ex2)
                                                {
                                                    num2 = new Decimal(0);
                                                }
                                            }
                                        }
                                    }
                                    if (Math.Abs(aServ.ServOdn.CCalc) > 0.00001m && aServ.Serv.Tarif > 0.001m)
                                    {
                                        string str2 = "(1)";
                                        if (aServ.Serv.NzpServ == 6 & HasHvsDpu)
                                            str2 = "(4)";
                                        if (aServ.Serv.NzpServ == 9 & HasGvsDpu)
                                            str2 = "(4)";
                                        if (aServ.Serv.NzpServ == 14 & HasGvsDpu)
                                            str2 = "(4)";
                                        if (aServ.Serv.NzpServ == 25 & HasElDpu)
                                            str2 = "(4)";
                                        if (aServ.Serv.NzpServ == 210)
                                            str2 = "(4)";

                                        объемКоммунальныхУслуг.ОбщедомовыеНужды = (aServ.ServOdn.CCalc.ToString("0.0000") + str2);
                                        try
                                        {
                                            num4 = aServ.ServOdn.CCalc;
                                        }
                                        catch (Exception ex)
                                        {
                                            num3 = new Decimal(0);
                                        }
                                    }
                                    if (aServ.Serv.NzpServ == 7 && aServ.Serv.NzpFrm == 26907209)
                                    {
                                        foreach (ServVolume servVolume in this.ListVolume)
                                        {
                                            if (servVolume.NzpServ == 7)
                                                servVolume.NormaVolume = this.KanNormCalc;
                                        }
                                    }
                                    if (Math.Abs(aServ.Serv.Tarif) > 0.001m)
                                    {
                                        услуга.Тариф = aServ.Serv.Tarif.ToString("0.000");
                                        try
                                        {
                                            num3 = aServ.Serv.Tarif;
                                        }
                                        catch (Exception ex)
                                        {
                                            num3 = new Decimal(0);
                                        }
                                    }
                                    Decimal num5;
                                    if (((aServ.Serv.NzpServ == 6 ? 1 : (aServ.Serv.NzpServ == 7 ? 1 : 0)) & (aServ.Serv.NzpMeasure != 3 ? 1 : 0)) != 0)
                                    {
                                        if (aServ.Serv.Norma > 0.001m)
                                        {
                                            услуга.Тариф = (aServ.Serv.Tarif / aServ.Serv.Norma).ToString("0.000");
                                        }
                                        услуга.ЕдиницаИзмерения = "Куб.м.";
                                    }
                                    РазмерПлатыЗаКоммунальныеУслуги размерПлатыЗаКоммунальныеУслуги = new РазмерПлатыЗаКоммунальныеУслуги();
                                    if (Math.Abs(aServ.Serv.RsumTarif - aServ.ServOdn.RsumTarif) > 0.001m)
                                    {
                                        if (num4 < new Decimal(0))
                                        {
                                            размерПлатыЗаКоммунальныеУслуги.ИндивидуальноеПотребление = Math.Round(num2 * num3, 2).ToString("0.00");
                                        }
                                        else
                                        {
                                            размерПлатыЗаКоммунальныеУслуги.ИндивидуальноеПотребление =
                                                (aServ.Serv.RsumTarif - aServ.ServOdn.RsumTarif).ToString("0.00");
                                        }
                                    }
                                    if (Math.Abs(aServ.ServOdn.RsumTarif) > 0.001m)
                                    {
                                        if (num4 < new Decimal(0))
                                        {
                                            размерПлатыЗаКоммунальныеУслуги.ОбщедомовыеНужды = Math.Round(num3 * num4, 2).ToString("0.00");
                                        }
                                        else if (aServ.ServOdn.RsumTarif > 0)
                                            размерПлатыЗаКоммунальныеУслуги.ОбщедомовыеНужды = aServ.ServOdn.RsumTarif.ToString("0.00");
                                        else
                                            размерПлатыЗаКоммунальныеУслуги.ОбщедомовыеНужды = "";
                                    }
                                    услуга.РазмерПлатыЗаКоммунальныеУслуги = размерПлатыЗаКоммунальныеУслуги;
                                    if (Math.Abs(aServ.Serv.RsumTarif) > new Decimal(1, 0, 0, false, (byte)3))
                                        услуга.ВсегоНачислено = aServ.Serv.RsumTarif.ToString("0.00");
                                    if (Math.Abs(aServ.Serv.Reval + aServ.Serv.RealCharge) > 0.001m)
                                    {
                                        услуга.Перерасчеты = (aServ.Serv.Reval + aServ.Serv.RealCharge).ToString("0.00");
                                    }
                                    else
                                    {
                                        услуга.Перерасчеты = "";
                                    }
                                    //if (Math.Abs(aServ.Serv.Reval + aServ.Serv.RealCharge) > Math.Abs(aServ.Serv.RsumTarif))
                                    //  num1 += aServ.Serv.Reval + aServ.Serv.RealCharge + aServ.Serv.RsumTarif;
                                    услуга.Льготы = "";
                                    //if (Math.Abs(aServ.Serv.SumCharge) > new Decimal(1, 0, 0, false, (byte)3))
                                    //   dr["sum_charge_all" + (object)index1] = (object)aServ.Serv.SumCharge.ToString("0.00");
                                    ИтогоКОплате итогоКОплпте = new ИтогоКОплате();
                                    ЗаКоммульныеУслуги заКоммунальныеУслуги = new ЗаКоммульныеУслуги();
                                    if (Math.Abs(aServ.Serv.SumCharge - aServ.ServOdn.SumCharge) > new Decimal(1, 0, 0, false, (byte)3))
                                    {
                                        заКоммунальныеУслуги.ИндивидуальноеПотребление = (aServ.Serv.SumCharge - aServ.ServOdn.SumCharge).ToString("0.00");
                                    }

                                    if (Math.Abs(aServ.ServOdn.SumCharge) > 0.001m)
                                    {
                                        if (num4 < new Decimal(0))
                                            заКоммунальныеУслуги.ОбщедомовыеНужды = "";
                                        else
                                            заКоммунальныеУслуги.ОбщедомовыеНужды = aServ.ServOdn.RsumTarif.ToString("0.00");
                                    }
                                    else
                                    {
                                        if (num4 < new Decimal(0))
                                            заКоммунальныеУслуги.ОбщедомовыеНужды = "";
                                        if (Math.Abs(aServ.ServOdn.RsumTarif) > 0.001m)
                                            заКоммунальныеУслуги.ОбщедомовыеНужды = aServ.ServOdn.RsumTarif.ToString("0.00");
                                    }
                                    итогоКОплпте.ЗаКоммульныеУслуги = заКоммунальныеУслуги;
                                    услуга.ИтогоКОплате = итогоКОплпте;
                                    //объемКоммунальныхУслуг = new ОбъемКоммунальныхУслуг();
                                    if (aServ.Serv.NzpMeasure == 4 & Math.Abs(aServ.Serv.RsumTarif) > new Decimal(1, 0, 0, false, (byte)3) && aServ.Serv.Tarif > 0.001m)
                                    {
                                        if (aServ.Serv.OldMeasure == 4)
                                        {
                                            if (aServ.Serv.NzpServ == 9)
                                            {
                                                if (Math.Abs(aServ.Serv.CCalc) > 0.00001m)
                                                    объемКоммунальныхУслуг.ИндивидуальноеПотребление = (aServ.Serv.CCalc.ToString("0.0000") +
                                                        GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice));
                                            }
                                            else if (!IsolateFlat && aServ.Serv.NzpServ == 8)//qqq
                                            {
                                                num2 = OtopNorm * LiveSquare;
                                                объемКоммунальныхУслуг.ИндивидуальноеПотребление = (OtopNorm * LiveSquare).ToString("0.0000") +
                                                    GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice);
                                            }
                                            else
                                                объемКоммунальныхУслуг.ИндивидуальноеПотребление =
                                                    (aServ.Serv.CCalc.ToString("0.0000") + GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice));
                                        }
                                        else if (aServ.Serv.NzpServ == 9)
                                        {
                                            if (Math.Abs(aServ.Serv.CCalc) > new Decimal(1, 0, 0, false, (byte)5))
                                                объемКоммунальныхУслуг.ИндивидуальноеПотребление =
                                                    ((aServ.Serv.CCalc * this.GvsNormGkal).ToString("0.0000") + GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice));
                                        }
                                        else
                                            объемКоммунальныхУслуг.ИндивидуальноеПотребление =
                                                ((aServ.Serv.CCalc * this.OtopNorm).ToString("0.0000") + GetVolumeSource(aServ.Serv.NzpServ, aServ.Serv.IsDevice));
                                        if (Math.Abs(aServ.ServOdn.CCalc) > 0.000001m)
                                        {
                                            string str2 = "(1)";
                                            if (HasGvsDpu)
                                                str2 = "(4)";
                                            if (aServ.Serv.NzpServ == 9)
                                                объемКоммунальныхУслуг.ОбщедомовыеНужды = (aServ.ServOdn.CCalc.ToString("0.0000") + str2);
                                            else
                                                объемКоммунальныхУслуг.ОбщедомовыеНужды = (aServ.ServOdn.CCalc.ToString("0.0000") + str2);
                                        }
                                    }
                                    услуга.ОбъемКоммунальныхУслуг = объемКоммунальныхУслуг;
                                    try
                                    {
                                        if (aServ.Serv.NameServ.Trim() != "Подогрев")
                                        {
                                            try
                                            {
                                                справочнаяИнформация = FillServiceVolume(справочнаяИнформация, aServ.Serv.NzpServ, aServ.Serv.NameServ.Trim(),
                                                услуга.ОбъемКоммунальныхУслуг.ИндивидуальноеПотребление != "" ?
                                                Convert.ToDecimal(услуга.ОбъемКоммунальныхУслуг.ИндивидуальноеПотребление.Split('(')[0]) : 0);
                                            }
                                            catch
                                            {
                                                справочнаяИнформация = FillServiceVolume(справочнаяИнформация, aServ.Serv.NzpServ, aServ.Serv.NameServ.Trim(),
                                                услуга.ОбъемКоммунальныхУслуг.ИндивидуальноеПотребление != "" ?
                                                Convert.ToDecimal(услуга.ОбъемКоммунальныхУслуг.ИндивидуальноеПотребление.Split('(')[0].Replace(",", ".")) : 0);
                                            }
                                            
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        //exception = ex;
                                    }
                                    if (aServ.Serv.NzpMeasure == 4 & Math.Abs(aServ.Serv.RsumTarif) > new Decimal(1, 0, 0, false, (byte)3) && aServ.Serv.Tarif > 0.001m)
                                    {
                                        справочнаяИнформация =
                                            FillGoodServVolume(справочнаяИнформация, aServ.Serv.NzpServ == 9 ? GvsNormGkal : OtopNorm,
                                            "rash_norm");
                                    }
                                    справочнаяИнформация.ВидУслуги = услуга.ВидУслуги;
                                    справочныеДанные.Add(справочнаяИнформация);
                                    if (aServ.Serv.NzpMeasure == 26)
                                    {
                                        услуга.ЕдиницаИзмерения = "";
                                        услуга.ОбъемКоммунальныхУслуг.ИндивидуальноеПотребление = "";
                                    }

                                    услуги.Add(услуга);
                                }
                            }
                        }
                        //dr["revalEpd"] = (object)num1.ToString();
                    }
                    //sw.WriteLine("4");
                    foreach (XmlClass.Услуга услуга in услуги)
                    {
                        Decimal rsumTarifAll = 0;
                        Decimal reval = 0;
                        Decimal kommServIndivid = 0;
                        Decimal itogoKommIndivid = 0;
                        Decimal itogoKommODN = 0;
                        if (услуга.РазмерПлатыЗаКоммунальныеУслуги.ИндивидуальноеПотребление != "")
                        {
                            try
                            {
                                kommServIndivid = Convert.ToDecimal(услуга.РазмерПлатыЗаКоммунальныеУслуги.ИндивидуальноеПотребление);
                            }
                            catch
                            {
                                kommServIndivid = Convert.ToDecimal(услуга.РазмерПлатыЗаКоммунальныеУслуги.ИндивидуальноеПотребление.Replace(",","."));
                            }
                        }
                            
                        if (услуга.Перерасчеты != "")
                        {
                            try
                            {
                                reval = Convert.ToDecimal(услуга.Перерасчеты);
                            }
                            catch
                            {
                                reval = Convert.ToDecimal(услуга.Перерасчеты.Replace(",", "."));
                            }
                        }
                            
                        if (услуга.ВсегоНачислено != "")
                        {
                            try
                            {
                                rsumTarifAll = Convert.ToDecimal(услуга.ВсегоНачислено);
                            }
                            catch
                            {
                                rsumTarifAll = Convert.ToDecimal(услуга.ВсегоНачислено.Replace(",", "."));
                            }
                        }
                            
                        if (услуга.ИтогоКОплате.ЗаКоммульныеУслуги.ИндивидуальноеПотребление != "")
                        {
                            try
                            {
                                itogoKommIndivid = Convert.ToDecimal(услуга.ИтогоКОплате.ЗаКоммульныеУслуги.ИндивидуальноеПотребление);
                            }
                            catch
                            {
                                itogoKommIndivid = Convert.ToDecimal(услуга.ИтогоКОплате.ЗаКоммульныеУслуги.ИндивидуальноеПотребление.Replace(",", "."));
                            }
                        }
                            
                        if (услуга.ИтогоКОплате.ЗаКоммульныеУслуги.ОбщедомовыеНужды != "")
                        {
                            try
                            {
                                itogoKommODN = Convert.ToDecimal(услуга.ИтогоКОплате.ЗаКоммульныеУслуги.ОбщедомовыеНужды);
                            }
                            catch
                            {
                                itogoKommODN = Convert.ToDecimal(услуга.ИтогоКОплате.ЗаКоммульныеУслуги.ОбщедомовыеНужды.Replace(",", "."));
                            }
                        }
                            
                        if (услуга.ВидУслуги == "Пени")
                        {
                            услуга.ОбъемКоммунальныхУслуг.ИндивидуальноеПотребление = "";
                            услуга.ЕдиницаИзмерения = "руб.";
                            услуга.Тариф = "";

                            if (kommServIndivid + reval > 0)
                            {
                                услуга.РазмерПлатыЗаКоммунальныеУслуги.ИндивидуальноеПотребление = (kommServIndivid + reval).ToString();
                            }
                            else
                            {
                                услуга.РазмерПлатыЗаКоммунальныеУслуги.ИндивидуальноеПотребление = "";
                            }

                            if (rsumTarifAll + reval > 0)
                            {
                                услуга.ВсегоНачислено = (rsumTarifAll + reval).ToString();
                            }
                            else
                            {
                                услуга.ВсегоНачислено = "";
                            }
                            услуга.Перерасчеты = "";
                        }
                        if (rsumTarifAll + reval > 0)
                        {
                            услуга.ИтогоКОплате.Всего = (rsumTarifAll + reval).ToString();
                        }
                        else
                        {
                            услуга.ИтогоКОплате.Всего = "";
                        }
                        if (услуга.ВидУслуги == "Пени")
                        {
                            Decimal t1 = 0;
                            Decimal t2 = 0;
                            if (rsumTarifAll + reval - itogoKommODN != 0)
                            {
                                t1 = rsumTarifAll + reval - itogoKommODN;
                            }
                            if (itogoKommIndivid != t1)
                            {
                                if (t1 > 0)
                                {
                                    услуга.ИтогоКОплате.ЗаКоммульныеУслуги.ИндивидуальноеПотребление = (rsumTarifAll + reval - itogoKommODN).ToString();
                                }
                            }


                        }
                        else
                        {
                            if (rsumTarifAll + reval - itogoKommODN != 0)
                            {
                                услуга.ИтогоКОплате.ЗаКоммульныеУслуги.ИндивидуальноеПотребление = (rsumTarifAll + reval - itogoKommODN).ToString();
                            }
                            else
                            {
                                услуга.ИтогоКОплате.ЗаКоммульныеУслуги.ИндивидуальноеПотребление = "";
                            }
                        }
                    }
                    //sw.WriteLine("5");
                    Итого итого = new Итого();
                    Decimal sum6 = 0;
                    Decimal sum7 = 0;
                    Decimal sum8 = 0;
                    Decimal sum9 = 0;
                    Decimal sum11 = 0;
                    Decimal sum12 = 0;
                    Decimal sum13 = 0;
                    foreach (XmlClass.Услуга услуга in услуги)
                    {
                        if (услуга.РазмерПлатыЗаКоммунальныеУслуги.ИндивидуальноеПотребление != "")
                        {
                            try
                            {
                                sum6 += Convert.ToDecimal(услуга.РазмерПлатыЗаКоммунальныеУслуги.ИндивидуальноеПотребление);
                            }
                            catch
                            {
                                sum6 += Convert.ToDecimal(услуга.РазмерПлатыЗаКоммунальныеУслуги.ИндивидуальноеПотребление.Replace(",", "."));
                            }
                        }
                            
                        if (услуга.РазмерПлатыЗаКоммунальныеУслуги.ОбщедомовыеНужды != "")
                        {
                            try
                            {
                                sum7 += Convert.ToDecimal(услуга.РазмерПлатыЗаКоммунальныеУслуги.ОбщедомовыеНужды);
                            }
                            catch
                            {
                                sum7 += Convert.ToDecimal(услуга.РазмерПлатыЗаКоммунальныеУслуги.ОбщедомовыеНужды.Replace(",", "."));
                            }
                        }
                        
                        if (услуга.ВсегоНачислено != "")
                        {
                            try
                            {
                                sum8 += Convert.ToDecimal(услуга.ВсегоНачислено);
                            }
                            catch
                            {
                                sum8 += Convert.ToDecimal(услуга.ВсегоНачислено.Replace(",", "."));
                            }
                        }
                        
                        if (услуга.Перерасчеты != "")
                        {
                            try
                            {
                                sum9 += Convert.ToDecimal(услуга.Перерасчеты);
                            }
                            catch
                            {
                                sum9 += Convert.ToDecimal(услуга.Перерасчеты.Replace(",", "."));
                            }
                        }
                        
                        if (услуга.ИтогоКОплате.Всего != "")
                        {
                            try
                            {
                                sum11 += Convert.ToDecimal(услуга.ИтогоКОплате.Всего);
                            }
                            catch
                            {
                                sum11 += Convert.ToDecimal(услуга.ИтогоКОплате.Всего.Replace(",", "."));
                            }
                        }
                        
                        if (услуга.ИтогоКОплате.ЗаКоммульныеУслуги.ИндивидуальноеПотребление != "")
                        {
                            try
                            {
                                sum12 += Convert.ToDecimal(услуга.ИтогоКОплате.ЗаКоммульныеУслуги.ИндивидуальноеПотребление);
                            }
                            catch
                            {
                                sum12 += Convert.ToDecimal(услуга.ИтогоКОплате.ЗаКоммульныеУслуги.ИндивидуальноеПотребление.Replace(",", "."));
                            }
                        }
                        
                        if (услуга.ИтогоКОплате.ЗаКоммульныеУслуги.ОбщедомовыеНужды != "")
                        {
                            try
                            {
                                sum13 += Convert.ToDecimal(услуга.ИтогоКОплате.ЗаКоммульныеУслуги.ОбщедомовыеНужды);
                            }
                            catch
                            {
                                sum13 += Convert.ToDecimal(услуга.ИтогоКОплате.ЗаКоммульныеУслуги.ОбщедомовыеНужды.Replace(",", "."));
                            }
                        }
                        
                    }
                    //sw.WriteLine("6");
                    итого.РазмерПлатыИндивид = sum6.ToString("0.00");
                    итого.РазмерПлатыДом = sum7.ToString("0.00");
                    итого.ВсегоНачислено = sum8.ToString("0.00");
                    итого.Перерасчеты = sum9.ToString("0.00");
                    итого.Всего = sum11.ToString("0.00");
                    итого.ИтогоИндивид = sum12.ToString("0.00");
                    итого.ИтогоДом = sum13.ToString("0.00");
                    итого.Долг = SummaryServ.Serv.SumInsaldo.ToString("0.00");
                    итого.Оплачено = SummaryServ.Serv.SumMoney.ToString("0.00");
                    //sw.WriteLine("7");
                    Decimal sumCharge = 0;
                    if (SummaryServ.Serv.RsumTarif + (SummaryServ.Serv.Reval + SummaryServ.Serv.RealCharge) + SummaryServ.Serv.SumInsaldo - SummaryServ.Serv.SumMoney > 0)
                    {
                        sumCharge =
                            SummaryServ.Serv.RsumTarif + (SummaryServ.Serv.Reval + SummaryServ.Serv.RealCharge) + SummaryServ.Serv.SumInsaldo - SummaryServ.Serv.SumMoney;
                    }
                    decimal otopDpu;
                    try
                    {
                        otopDpu = (DomSquare - MopSquare) * Convert.ToDecimal(RashDpuPu.Trim());
                    }
                    catch
                    {
                        otopDpu = (DomSquare - MopSquare) * Convert.ToDecimal(RashDpuPu.Trim().Replace('.', ','));
                    }
                    //sw.WriteLine("8");
                    foreach (СправочнаяИнформация справочнаяИнформация in справочныеДанные)
                    {
                        if (справочнаяИнформация.ВидУслуги.Contains("Отопление"))
                        {
                            справочнаяИнформация.ОбъемКоммунальныхУслуг4.ПомещенияДома = otopDpu.ToString("0.00");
                        }
                    }
                    //sw.WriteLine("9");
                    раздел2.СуммаКОплате = sumCharge.ToString("0.00");
                    раздел4.СправочныеДанные = справочныеДанные;
                    раздел3.Услуги = услуги;
                    раздел3.Итого = итого;
                    лс.Раздел3 = раздел3;
                    лс.Раздел4 = раздел4;
                    DataTable remark = SelectRemark(db, part1.Rows[i]["nzp_dom"].ToString(), part1.Rows[i]["nzp_geu"].ToString(), part1.Rows[i]["nzp_area"].ToString());
                    лс.Примечание = remark.Rows[0]["remark"].ToString();
                    returnData.Add(лс);
                    //sw.WriteLine("10");
                }
                DropTable("t_fkvar_prm", db);
                DropTable("t_freasonReval", db);
                DropTable("t_fVolume", db);
                DropTable("t_fDomVolume", db);
                DropTable("t_fPerekidka", db);
                conn.Close();
            }
            catch(Exception ex)
            {
                conn.Close();
                returnData = new List<ЛицевойСчет>();
                ЛицевойСчет лс= new ЛицевойСчет();
                лс.Примечание = "Id квартиры = " + nzp_kvar + "|||" + tempVal + "|||ошибка = " + ex.ToString();
                returnData.Add(лс);
            }
            //sw.Close();
            return returnData;
        }

        protected СправочнаяИнформация FillServiceVolume(СправочнаяИнформация справочнаяИнформация, int nzpServ, string nameServ, Decimal c_calc)
        {
            //aServ.ServOdn.CCalc
            //СправочнаяИнформация справочнаяИнформация = new СправочнаяИнформация();
            //справочнаяИнформация.ВидУслуги = nameServ;
            //НормативПотребления нормативПотребления = new НормативПотребления();
            int num1 = nzpServ;
            int num2 = nzpServ;
            if (nzpServ == 99)
                return new СправочнаяИнформация();
            if (nzpServ == 14)
                num1 = 9;
            int index1 = -1;
            for (int index2 = 0; index2 < ListVolume.Count; ++index2)
            {
                if (ListVolume[index2].NzpServ == num1)
                    index1 = index2;
            }
            if (index1 == -1)
                return new СправочнаяИнформация();
            if (num1 == 25 & KfodnEl != "")
                справочнаяИнформация.НормативПотребления.Общедомовое = KfodnEl;
            if (nzpServ == 14 & Kfodngvs != "")
                справочнаяИнформация.НормативПотребления.Общедомовое = Kfodngvs;
            if (num1 == 6 & Kfodnhvs != "" & HasHvsDpu)
                справочнаяИнформация.НормативПотребления.Общедомовое = Kfodnhvs;
            if (num1 == 210 & HasElDpu || (num1 == 10 & HasGazDpu || num1 == 6 & this.HasGazDpu) || num2 == 14 & this.HasGvsDpu || num1 == 8 & this.HasOtopDpu)
            {
                справочнаяИнформация = FillGoodServVolume(справочнаяИнформация, ListVolume[index1].DomVolume, "rash_dpu_pu");
                справочнаяИнформация = FillGoodServVolume(справочнаяИнформация, ListVolume[index1].OdnDomVolume, "rash_dpu_odn");
            }
            else if (num1 == 6 & HasHvsDpu)
            {
                //this.FillGoodServVolume4(dr, this.ListVolume[index1].OdnDomVolume, "rash_dpu_odn" + (object)index);
                справочнаяИнформация = FillGoodServVolume(справочнаяИнформация, ListVolume[index1].DomVolume, "rash_dpu_pu");
            }

            if (!(num1 == 25 & c_calc.ToString().Trim() == ""))
                справочнаяИнформация = FillGoodServVolume(справочнаяИнформация, ListVolume[index1].NormaVolume, "rash_norm");
            decimal sumCountersValue = 0;
            string countersVal = "";
            for (int k = 0; k < ListCounters.Count; k++)
            {
                if ((ListCounters[k].NzpServ == num1))
                {
                    if (countersVal.Length == 0)
                        countersVal += ListCounters[k].Value.ToString();
                    else
                        countersVal += "/" + ListCounters[k].Value.ToString();

                }
            }
            //FillGoodServVolume(dr, sumCountersValue, "rash_pu" + index);
            Показания показания = new Показания();
            if (nzpServ != 9)
                показания.Индивидуальные = countersVal;

            string domCountersValue = "";
            decimal sumDomCountersValue = 0;

            //sw.WriteLine("anzpServ = " + anzpServ);
            for (int k = 0; k < ListDomCounters.Count; k++)
            {
                //sw.WriteLine("ListDomCounters[k].NzpServ = " + ListDomCounters[k].NzpServ);
                if ((ListDomCounters[k].NzpServ == num1))
                {
                    sumCountersValue += ListDomCounters[k].Value;
                    sumDomCountersValue += ListDomCounters[k].Value - ListDomCounters[k].ValuePred;
                    if (domCountersValue.Length == 0)
                        domCountersValue += ListDomCounters[k].Value.ToString();
                    else
                        domCountersValue += "/" + ListDomCounters[k].Value.ToString();
                }
            }
            if (nzpServ != 8)
            {
                if (domCountersValue.Length != 0)
                    показания.Общедомовые = domCountersValue;
                else
                    показания.Общедомовые = "";
            }
            справочнаяИнформация.Показания = показания;
            if (num1 == 25 & HasElDpu)
            {
                справочнаяИнформация = FillGoodServVolume(справочнаяИнформация, ListVolume[index1].DomVolume, "rash_dpu_pu");
                if (domCountersValue.Length != 0)
                {
                    if (ListVolume[index1].DomLiftVolume != 0)
                    {
                        decimal val = ListVolume[index1].DomLiftVolume - ListVolume[index1].DomVolume;
                        справочнаяИнформация = FillGoodServVolume(справочнаяИнформация, val, "rash_dpu_odn");

                    }
                    else
                    {
                        справочнаяИнформация = FillGoodServVolume(справочнаяИнформация, 0, "rash_dpu_odn");
                    }
                }
                else
                {
                    справочнаяИнформация = FillGoodServVolume(справочнаяИнформация,
                    ListVolume[index1].DomLiftVolume != 0
                        ? ListVolume[index1].DomLiftVolume
                        : ListVolume[index1].OdnDomVolume, "rash_dpu_odn" );
                }


            }
            else if (num1 == 25)
            {
                справочнаяИнформация = FillGoodServVolume(справочнаяИнформация, ListVolume[index1].DomVolume, "rash_dpu_pu");
                if (domCountersValue.Length != 0)
                {
                    if (ListVolume[index1].DomLiftVolume != 0)
                    {
                        decimal val = ListVolume[index1].DomLiftVolume - ListVolume[index1].DomVolume;
                        справочнаяИнформация = FillGoodServVolume(справочнаяИнформация, val, "rash_dpu_odn");

                    }
                    else
                    {
                        справочнаяИнформация = FillGoodServVolume(справочнаяИнформация, 0, "rash_dpu_odn");
                    }
                }
                else
                {
                    справочнаяИнформация = FillGoodServVolume(справочнаяИнформация,
                    ListVolume[index1].DomLiftVolume != 0
                        ? ListVolume[index1].DomLiftVolume - ListVolume[index1].DomVolume
                        : ListVolume[index1].OdnDomVolume - ListVolume[index1].DomVolume, "rash_dpu_odn");
                }
            }

            if (nzpServ == 9 & NumberDom != "94")
                справочнаяИнформация = FillGoodServVolume(справочнаяИнформация, this.ListVolume[index1].OdnDomVolume, "rash_dpu_odn");
            return справочнаяИнформация;
        }

        protected СправочнаяИнформация FillGoodServVolume(СправочнаяИнформация справочнаяИнформация, Decimal aValue, string colName)
        {
            if (Math.Abs(aValue) < 0.0001m)
            {
                switch (colName)
                {
                    case "rash_dpu_pu":
                        {
                            справочнаяИнформация.ОбъемКоммунальныхУслуг4.ПомещенияДома = "";
                            break;
                        }
                    case "rash_dpu_odn":
                        {
                            справочнаяИнформация.ОбъемКоммунальныхУслуг4.ОбщедомовыеНуждыДома = "";
                            break;
                        }
                    case "rash_norm":
                        {
                            справочнаяИнформация.НормативПотребления.Индивидуальное = "";
                            break;
                        }
                }
            }
            else
            {
                switch (colName)
                {
                    case "rash_dpu_pu":
                        {
                            справочнаяИнформация.ОбъемКоммунальныхУслуг4.ПомещенияДома = aValue.ToString();
                            break;
                        }
                    case "rash_dpu_odn":
                        {
                            справочнаяИнформация.ОбъемКоммунальныхУслуг4.ОбщедомовыеНуждыДома = aValue.ToString("0.00");
                            break;
                        }
                    case "rash_norm":
                        {
                            справочнаяИнформация.НормативПотребления.Индивидуальное = aValue.ToString("0.0000");
                            break;
                        }
                }
            }
            return справочнаяИнформация;
                //dr[colName] = colName.IndexOf("rash_pu", StringComparison.Ordinal) <= -1 ? (colName.IndexOf("rash_dpu_pu", StringComparison.Ordinal) <= -1 ? (colName.IndexOf("rash_dpu_odn", StringComparison.Ordinal) <= -1 ? (colName.IndexOf("rash_norm", StringComparison.Ordinal) <= -1 ? (object)aValue.ToString("0.0000") : (object)aValue.ToString("0.00##")) : (object)aValue.ToString("0.00")) : (object)aValue.ToString("0.00")) : (object)aValue.ToString("0.00");
        }

        public bool IsShowServInGrid(BaseServ aServ)
        {
            if (
                (System.Math.Abs(aServ.Serv.RsumTarif) < 0.001m) &
                (System.Math.Abs(aServ.Serv.Reval) < 0.001m) &
                (System.Math.Abs(aServ.Serv.RealCharge) < 0.001m) &
                (System.Math.Abs(aServ.Serv.CCalc) < 0.001m) &
                (System.Math.Abs(aServ.Serv.SumCharge) < 0.001m)
            )
            {
                return false;
            }
            return true;
        }

        protected string GetVolumeSource(int nzpServ, int isPu)
        {
            int aserv = nzpServ;
            if (aserv == 14) aserv = 9;
            if (nzpServ == 7) aserv = 6;

            if (nzpServ == 7)
            {
                foreach (BaseServ t in ListServ)
                {
                    if (t.Serv.NzpServ == 6)
                        isPu = t.Serv.IsDevice;
                }
            }

            if ((aserv == 6) || (aserv == 9) || (aserv == 25))
            {
                switch (isPu)
                {
                    case 0: return "(1)";
                    case 1: return "(2)";
                    case 9: return "(3)";
                }
                return "(1)";
            }

            return "";
        }
        protected List<BaseServ> SortServ(List<BaseServ> baseList)
        {
            List<BaseServ> returnList = new List<BaseServ>();
            int pos = 0;
            int posAfter = 0;
            foreach (BaseServ bs in baseList)
            {
                if (bs.Serv.NzpServ == 9)
                {
                    returnList.Insert(pos, bs);
                    posAfter = pos + 1;
                }
                else if (bs.Serv.NzpServ == 14)
                {
                    returnList.Insert(posAfter, bs);
                }
                else
                {
                    returnList.Insert(pos, bs);
                }
                pos++;
            }
            return returnList;
        }

        public virtual void AddVolume(ServVolume aVolume)
        {
            ListVolume.Add(aVolume);
        }

        protected virtual bool SetServRashod()
        {
            decimal kanNorma = 0;
            decimal gvsNorm = 0;

            int numhvsgvs = -1;

            foreach (ServVolume t in ListVolume)
            {
                for (int j = 0; j < ListServ.Count; j++)
                {
                    if ((t.NzpServ == ListServ[j].Serv.NzpServ) & (t.NzpServ != 8) &
                        (t.NzpServ != 7))
                    {
                        if (t.IsPu > 0)
                        {
                            ListServ[j].Serv.CCalc = t.PUVolume;
                            ListServ[j].ServOdn.CCalc = t.OdnFlatPuVolume;
                        }
                        else
                        {

                            ListServ[j].Serv.CCalc = t.NormaFullVolume;
                            ListServ[j].ServOdn.CCalc = t.OdnFlatNormVolume;
                        }

                        ListServ[j].Serv.Norma = t.NormaVolume;

                        if (ListServ[j].Serv.NzpServ == 14)
                        {
                            numhvsgvs = j;
                            gvsNorm = t.NormaVolume;
                        }
                        if (ListServ[j].Serv.NzpServ == 6)
                        {
                            kanNorma = t.NormaVolume;
                        }

                    }
                }
            }

            //Если есть ХВС для ГВС
            if (numhvsgvs > -1)
            {
                //Норматив 9-ке проставляем кубометровый
                foreach (ServVolume t in ListVolume)
                    if (t.NzpServ == 9) t.NormaVolume = gvsNorm;


                //Норматив канализации проставляем как сумму ХВС и ГВС
                foreach (ServVolume t in ListVolume)
                    if (t.NzpServ == 7) t.NormaVolume = gvsNorm + kanNorma;
            }


            //Для канализации проставляем кубометры в нормативе если расчет с человека
            foreach (BaseServ t in ListServ)
            {
                if ((t.Serv.NzpServ == 7) & (t.Serv.OldMeasure == 2))
                {
                    t.Serv.Norma = kanNorma;
                    t.Serv.CCalc = t.Serv.CCalc * kanNorma;
                }
            }

            return true;

        }

        public void GetListKommServ(String database)
        {
            string cmdText = @"SELECT nzp_serv   FROM bill01_kernel.grpserv_schet a  WHERE nzp_grpserv = 2 ";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);
            /*NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            try
            {
                da.Fill(dt);
            }
            catch
            {
                return null;
            }*/
            IDataReader reader = null;
            try
            {
                reader = cmd.ExecuteReader();
                while (reader.Read())
                {


                    if (reader["nzp_serv"] != DBNull.Value)
                    {
                        var bs = new BaseServ(false)
                        {
                            Serv = { NzpServ = Convert.ToInt32(reader["nzp_serv"]) },
                            KommServ = true
                        };
                        ListKommServ.Add(bs);
                    }
                }
            }
            catch (Exception ex)
            {
                
            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader.Dispose();
                }
                if (cmd != null)
                {
                    cmd.Dispose();
                }
            }
        }

        public void GetRashDpuPu(String database, String nzp_kvar)
        {
            string cmdText = @"SELECT nzp, val_prm, dat_s, dat_po FROM bill01_data.prm_2   where nzp_prm = 2074 AND is_actual <> 100 and dat_po >= current_date 
AND nzp in (SELECT nzp_dom from bill01_data.kvar where nzp_kvar = "+nzp_kvar+")";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);

            IDataReader reader = null;
            try
            {
                reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    if (reader["val_prm"] != DBNull.Value)
                    {
                        RashDpuPu = reader["val_prm"].ToString();
                    }

                }
                reader.Close();
                reader.Dispose();
                cmd.Dispose();
            }
            catch (Exception e)
            {
                cmd.Dispose();
            }
        }

        public void FillServise(String database, Int32 m, Int32 y, String nzp_kvar)
        {
            LoadFormulList(database);
            string cmdText = @"SELECT s.ordering, s.service_name as service, m.measure, su.name_supp as name_supp, a.tarif,        
a.nzp_serv, m.nzp_measure, a.nzp_frm, a.nzp_supp, max(a.is_device) as is_device,         sum(a.gsum_tarif) as rsum_tarif, 
sum(a.sum_charge) as sum_charge,                      sum(a.rsum_tarif - a.gsum_tarif + a.reval - a.sum_nedop) as reval,                     
sum(0) as sum_sn, sum(a.real_charge) - coalesce(sum(p.sum_rcl), 0) as real_charge, 
max(a.c_calc) as c_calc,          sum(a.sum_money) as sum_money, sum(a.rsum_tarif - a.gsum_tarif) as reval_gil,          sum(a.sum_insaldo) as sum_insaldo,  
max(a.c_reval) as c_reval,                         sum(a.sum_nedop) as sum_nedop,  sum(a.sum_outsaldo) as sum_outsaldo            
FROM  bill01_charge_"+(y-2000).ToString("00")+".charge_"+m.ToString("00")+ @" a  LEFT JOIN (SELECT nzp_serv, nzp_supp, sum(sum_rcl) as sum_rcl  
FROM bill01_charge_" + (y - 2000).ToString("00") + @".perekidka p  INNER JOIN fbill_data.document_base d on d.nzp_doc_base = p.nzp_doc_base  
WHERE nzp_kvar="+ nzp_kvar + " AND d.comment = 'Выравнивание сальдо' and p.nzp_user = 1 and month_ = "+m+@" group by 1,2) p on p.nzp_supp = a.nzp_supp 
and p.nzp_serv  = a.nzp_serv, fbill_kernel.services s, fbill_kernel.s_measure m, fbill_kernel.supplier su  WHERE a.nzp_kvar="+ nzp_kvar + 
@" AND a.nzp_serv=s.nzp_serv  AND a.nzp_serv<>268  AND a.dat_charge is null AND a.nzp_serv>1 AND a.nzp_supp = su.nzp_supp         
AND s.nzp_measure=m.nzp_measure GROUP BY 1,2,3,4,5,6,7,8,9 ORDER BY ordering,nzp_serv, nzp_frm desc";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);
            /*NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            try
            {
                da.Fill(dt);
            }
            catch
            {
                return null;
            }*/
            IDataReader reader = null;
            try
            {
                reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    BaseServ serv;
                    if ((Int32.Parse(reader["nzp_serv"].ToString()) > 509) & (Int32.Parse(reader["nzp_serv"].ToString()) < 518))
                    {
                        serv = new BaseServ(true);
                    }
                    else
                    {
                        serv = new BaseServ(false);
                    }
                    serv.Serv.NzpServ = Int32.Parse(reader["nzp_serv"].ToString());

                    serv.Serv.NameServ = reader["service"].ToString().Trim();
                    if (((serv.Serv.NzpServ > 5) & (serv.Serv.NzpServ < 11)) || (serv.Serv.NzpServ == 25) ||
                        ((serv.Serv.NzpServ > 509) & (serv.Serv.NzpServ < 518)))
                    {
                        serv.Serv.NameSupp = "-" + reader["name_supp"].ToString().Trim();
                    }

                    if ((serv.Serv.NzpServ == 14) & (Int32.Parse(reader["nzp_supp"].ToString()) == 612))
                    {
                        serv.Serv.NameServ = "Хол.вода на ГВС";
                    }

                    if ((serv.Serv.NzpServ == 14))
                    {
                        serv.Serv.NameSupp = "-" + reader["name_supp"].ToString().Trim();
                    }

                    serv.Serv.Ordering = Int32.Parse(reader["ordering"].ToString());
                    serv.Serv.NzpMeasure = Int32.Parse(reader["nzp_measure"].ToString());
                    serv.Serv.NzpFrm = Convert.ToInt32(reader["nzp_frm"]);

                    serv.Serv.Measure = reader["measure"].ToString().Trim();
                    if (serv.Serv.NzpServ == 9)
                        if (Convert.ToInt32(reader["nzp_frm"]) == 0)
                        {
                            serv.Serv.NzpMeasure = 4;
                        }
                    if (serv.Serv.NzpServ == 8)
                        if (Convert.ToInt32(reader["nzp_frm"]) == 0)
                        {
                            serv.Serv.NzpMeasure = 4;
                        }
                    if (serv.Serv.NzpMeasure == 4) serv.Serv.Measure = "Гкал.";
                    GetMeasureByFrm(Convert.ToInt32(reader["nzp_frm"]), ref serv.Serv.Measure, ref serv.Serv.NzpMeasure);
                    serv.Serv.OldMeasure = serv.Serv.NzpMeasure;
                    serv.Serv.Tarif = reader["tarif"] != DBNull.Value ? Convert.ToDecimal(reader["tarif"]) : 0; // Добавил Андрей Кайнов 19.12.2012

                    serv.Serv.IsDevice = Int32.Parse(reader["is_device"].ToString());
                    serv.Serv.RsumTarif = Decimal.Parse(reader["rsum_tarif"].ToString());
                    serv.Serv.SumNedop = Decimal.Parse(reader["sum_nedop"].ToString());
                    serv.Serv.Reval = Decimal.Parse(reader["reval"].ToString());
                    serv.Serv.RevalGil = Decimal.Parse(reader["reval_gil"].ToString());
                    serv.Serv.RealCharge = Decimal.Parse(reader["real_charge"].ToString());
                    serv.Serv.SumCharge = Decimal.Parse(reader["sum_charge"].ToString());
                    serv.Serv.SumMoney = Decimal.Parse(reader["sum_money"].ToString());
                    serv.Serv.SumInsaldo = Decimal.Parse(reader["sum_insaldo"].ToString());
                    serv.Serv.SumSn = Decimal.Parse(reader["sum_sn"].ToString());
                    serv.Serv.SumOutsaldo = Decimal.Parse(reader["sum_outsaldo"].ToString());
                    serv.Serv.CCalc = Decimal.Parse(reader["c_calc"].ToString());
                    serv.Serv.CReval = Decimal.Parse(reader["c_reval"].ToString());
                    if (serv.Serv.Tarif <= Convert.ToDecimal(0.0001)) serv.Serv.CCalc = 0; // Добавил Андрей Кайнов 19.12.2012

                    //if (convertedTarifs != null)
                    //{
                    //    convertedTarifs.ReplaceServiceFrm(ref serv, Convert.ToInt32(reader["nzp_frm"]));
                    //}

                    serv.CopyToOdn();
                    AddServ(serv);
                }

            }
            catch (Exception e)
            {
                
            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader.Dispose();
                }
                if (cmd != null)
                {
                    cmd.Dispose();
                }
            }

        }

        public void FillCunionServ(String database)
        {
            CUnionServ.MasterList.Clear();
            string cmdText = @"SELECT s1.ordering as ord_base, s1.service_name as serv_base,         s1.ed_izmer as ed_izmer_base, a.nzp_serv_base,         
s2.ordering as ord_uni, s2.service_name as serv_uni,         s2.ed_izmer as ed_izmer_uni, a.nzp_serv_uni  
FROM  fbill_kernel.service_union a, fbill_kernel.services s1, fbill_kernel.services s2  WHERE a.nzp_serv_base=s1.nzp_serv        
AND a.nzp_serv_uni=s2.nzp_serv";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);
            /*NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            try
            {
                da.Fill(dt);
            }
            catch
            {
                return null;
            }*/
            IDataReader reader = null;
            try
            {
                reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    var servBase = new BaseServ(false)
                    {
                        Serv =
                                    {
                                        NzpServ = Int32.Parse(reader["nzp_serv_base"].ToString()),
                                        NameServ = reader["serv_base"].ToString(),
                                        Measure = reader["ed_izmer_base"].ToString(),
                                        Ordering = Int32.Parse(reader["ord_base"].ToString())
                                    }
                    };
                    var servUni = new BaseServ(false)
                    {
                        Serv =
                                    {
                                        NzpServ = Int32.Parse(reader["nzp_serv_uni"].ToString()),
                                        NameServ = reader["serv_uni"].ToString(),
                                        Measure = reader["ed_izmer_uni"].ToString(),
                                        Ordering = Int32.Parse(reader["ord_uni"].ToString())
                                    }
                    };
                    CUnionServ.AddServ(servBase, servUni);
                }
                reader.Close();
                reader.Dispose();
                cmd.Dispose();
            }
            catch(Exception e)
            {
                cmd.Dispose();
            }
        }

        public void FillDomServise(String database, Int32 m, Int32 y, String nzp_kvar, String nzp_dom)
        {
            string cmdText = @"SELECT s.service_name as service, a.nzp_serv, a.kod_info, a.cnt_stage,         a.dlt_calc,         a.kf_dpu_ls as dpu_odn,         
a.val1 + a.val2 + a.dlt_reval + a.dlt_real_charge as rashod,         
     case when a.cur_zap = 1 then (case when a.kod_info>100 then a.val3 else a.val4 end)-a.val1 -a.val2 -a.dlt_reval         
-a.dlt_real_charge else (case when a.kod_info>100 then a.val3 else a.val4 end) end as dpu,         
     a.val3 as dpu_cut,         coalesce(a.vl210,0) as norm_odn,         a.kf307,          a.cur_zap as counter_mop 
FROM bill01_charge_"+(y-2000).ToString("00")+".counters_"+m.ToString("00")+@" a,bill01_kernel.services s  WHERE a.nzp_dom = "+nzp_dom+ @"        
     AND dat_charge is null         AND a.nzp_serv=s.nzp_serv AND   a.nzp_serv != 8          AND stek = 3         AND nzp_type=1 
UNION ALL  
SELECT s.service_name as service, a.nzp_serv, a.kod_info, a.cnt_stage,         
     a.dlt_calc as dlt_calc,         a.kf_dpu_ls as dpu_odn,         a.val1 + a.val2 + a.dlt_reval + a.dlt_real_charge as rashod,         
case when a.cur_zap = 1 then (case when a.kod_info>100 then a.val3 else a.val4 end)-
     a.val1 -a.val2 -a.dlt_reval         -a.dlt_real_charge else (case when a.kod_info>100 then a.val3 else a.val4 end) end as dpu,         a.val3 as dpu_cut,    
coalesce(a.vl210,0) as norm_odn,         a.kf307,          
     a.cur_zap as counter_mop 
FROM bill01_charge_" + (y - 2000).ToString("00") + ".counters_" + m.ToString("00") + @" a,bill01_kernel.services s  WHERE a.nzp_dom = " + nzp_dom + @"       
AND dat_charge is null         AND a.nzp_serv=s.nzp_serv AND  a.nzp_serv = 8         AND stek = 9  AND nzp_type=1";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);

            decimal domBValueForHvsNaGvs = 0;
            IDataReader reader = null;
            try
            {
                reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    var servVolume = new ServVolume
                    {
                        DomArendatorsVolume = 0,
                        DomLiftVolume = Decimal.Parse(reader["dpu_cut"].ToString()),
                        NzpServ = Int32.Parse(reader["nzp_serv"].ToString()),
                        ServiceName = reader["service"].ToString().Trim()
                    };


                    if (reader["cnt_stage"].ToString() == "1" || (reader["cnt_stage"].ToString() == "5" && servVolume.NzpServ == 8)
                        || (reader["cnt_stage"].ToString() == "1" && servVolume.NzpServ == 9)
                        || (reader["cnt_stage"].ToString() == "0" && servVolume.NzpServ == 6)
                        || (m == 1 && y == 2015))
                    {
                        switch (servVolume.NzpServ)
                        {
                            case 25: HasElDpu = true; break;
                            case 6: HasHvsDpu = true; break;
                            case 9: HasGvsDpu = true; break;
                            case 10: HasGazDpu = true; break;
                            case 8: HasOtopDpu = true; break;
                        }
                    }

                    if (((servVolume.NzpServ == 88) & (reader["kod_info"].ToString() == "24")) ||
                        ((servVolume.NzpServ == 99) & (reader["kod_info"].ToString() == "25")))
                    {

                    }
                    else
                    {
                        if (m == 1 && y == 2015)
                        {
                            servVolume.AllLsVolume = Decimal.Parse(reader["dlt_calc"].ToString());
                            servVolume.OdnDomVolume = Decimal.Parse(reader["dpu_odn"].ToString());
                            servVolume.DomVolume = Decimal.Parse(reader["rashod"].ToString());
                            servVolume.DomLiftVolume = Decimal.Parse(reader["dpu_cut"].ToString());
                        }
                        else
                        {
                            servVolume.AllLsVolume = Decimal.Parse(reader["dlt_calc"].ToString());
                            servVolume.DomVolume = Decimal.Parse(reader["dpu"].ToString());
                            servVolume.OdnDomVolume = Decimal.Parse(reader["dpu_odn"].ToString());
                            servVolume.DomLiftVolume = Decimal.Parse(reader["dpu_cut"].ToString());
                            if (1 == 1)
                            {
                                if (servVolume.NzpServ == 999999)
                                    servVolume.OdnDomVolume = servVolume.DomVolume -
                                        Decimal.Parse(reader["rashod"].ToString());

                                servVolume.DomVolume = Decimal.Parse(reader["rashod"].ToString());

                                if (reader["counter_mop"].ToString() == "1")
                                {
                                    //servVolume.DomVolume = 0;
                                    servVolume.OdnDomVolume = Decimal.Parse(reader["dpu"].ToString());
                                }

                            }
                            else
                            {
                                servVolume.DomVolume = Decimal.Parse(0 == 111
                                    ? reader["rashod"].ToString()
                                    : reader["dpu_cut"].ToString());
                            }
                        }
                    }
                    if (reader["counter_mop"].ToString() == "1" && !(m == 1 && y == 2015))
                    {
                        //servVolume.DomVolume = 0;
                        servVolume.OdnDomVolume = Decimal.Parse(reader["dpu"].ToString());
                    }

                    if ((servVolume.NzpServ == 9) & (Decimal.Parse(reader["norm_odn"].ToString()) > 0.00001m))
                        Kfodngvs = Decimal.Parse(reader["norm_odn"].ToString()).ToString("0.00##");
                    else if ((servVolume.NzpServ == 6) & (Decimal.Parse(reader["norm_odn"].ToString()) > 0.00001m))
                        Kfodnhvs = Decimal.Parse(reader["norm_odn"].ToString()).ToString("0.00##");
                    else if ((servVolume.NzpServ == 25) & (Decimal.Parse(reader["norm_odn"].ToString()) > 0.00001m))
                        KfodnEl = Decimal.Parse(reader["norm_odn"].ToString()).ToString("0.00##");

                    servVolume.Kf307 = Decimal.Parse(reader["norm_odn"].ToString());
                    if (servVolume.NzpServ != 7)
                    {
                        //sw1.WriteLine(servVolume.ServiceName);
                        //sw1.WriteLine(servVolume.OdnDomVolume);
                        //sw1.WriteLine(servVolume.DomVolume);
                        //sw1.Close();
                        if (servVolume.ServiceName == "Горячая вода")
                        {
                            //var servVolume1 = new ServVolume
                            //{
                            //    DomArendatorsVolume = 0,
                            //    DomLiftVolume = 0,
                            //    NzpServ = 14,
                            //    ServiceName = "ХВС для ГВС",
                            //    AllLsVolume = 0,
                            //    OdnDomVolume = 0,
                            //    DomVolume = Decimal.Parse(reader["rashod"].ToString())
                            //};
                            //bill.AddDomVolume(servVolume1);
                        }
                        AddDomVolume(servVolume);
                    }
                }
                //sw.Close();
                reader.Close();
                reader.Dispose();
                cmd.Dispose();

            }
            catch (Exception ex)
            {
                cmd.Dispose();
            }
        }

        public void FillDomCounters(String database, Int32 m, Int32 y, String nzp_kvar, String nzp_dom)
        {
            DateTime firstDayNextMonth = Convert.ToDateTime("01." + m + "." + y).AddMonths(1);
            string cmdText = @"Select s.ordering, service_name as service, a.nzp_serv, a.num_cnt,  a.nzp_cnttype, a.dat_uchet, a.val_cnt,  b.num_cnt as num_cnt2, 
b.dat_uchet as dat_uchet2,  b.val_cnt as val_cnt2, sc.cnt_stage, formula, cs.dat_prov,  cs.dat_provnext, a.is_gkal, sm.measure  
From bill01_kernel.services s,                   bill01_kernel.s_counttypes sc,  bill01_data.counters_spis cs,  bill01_kernel.s_counts st,
bill01_kernel.s_measure sm,                  bill01_data.counters_dom a left join                  bill01_data.counters_dom b on                      
a.nzp_counter=b.nzp_counter                 AND b.dat_uchet<a.dat_uchet                 AND a.is_actual=b.is_actual                 
AND b.dat_uchet=(               Select max(dat_uchet) From bill01_data.counters_dom c               Where a.nzp_counter=c.nzp_counter                 
AND c.dat_uchet<a.dat_uchet                 AND c.is_actual = 1)           
Where a.nzp_serv=s.nzp_serv AND a.nzp_counter=cs.nzp_counter            AND cs.nzp_cnt=st.nzp_cnt AND st.nzp_measure=sm.nzp_measure             
AND a.nzp_cnttype=sc.nzp_cnttype             AND a.dat_close is null             AND a.is_actual=1             AND a.nzp_dom="+nzp_dom+@"            
AND a.dat_uchet>=Date('01.01.2015')             AND a.dat_uchet<='"+ firstDayNextMonth.ToShortDateString() + @"'            
AND a.dat_uchet=(               Select max(dat_uchet) From bill01_data.counters_dom c               
Where a.nzp_counter=c.nzp_counter                 AND c.dat_uchet<='"+ firstDayNextMonth.ToShortDateString() + @"'                
AND c.is_actual = 1 )             AND 0=(               Select count(*) From bill01_data.counters_spis d                                                       
Where a.nzp_counter=d.nzp_counter                                                                                     
AND d.is_actual = 1 AND d.dat_close is not null)                                        ORDER BY ordering,2,4,5";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);

            decimal domBValueForHvsNaGvs = 0;
            IDataReader reader = null;
            try
            {
                reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    var counters = new Counters
                    {
                        NzpServ = Int32.Parse(reader["nzp_serv"].ToString()),
                        Measure = reader["measure"].ToString().Trim(),
                        ServiceName = reader["service"].ToString().Trim(),
                        Value = Decimal.Parse(reader["val_cnt"].ToString()),
                        DatUchet = (DateTime)reader["dat_uchet"],
                        ValuePred =
                            reader["val_cnt2"] != DBNull.Value
                                ? Decimal.Parse(reader["val_cnt2"].ToString())
                                : 0,
                        NumCounters = reader["num_cnt"].ToString().Trim(),
                        DatUchetPred =
                            reader["dat_uchet2"] != DBNull.Value
                                ? (DateTime)reader["dat_uchet2"]
                                : DateTime.Parse("01.01.1900"),
                        CntStage = Int32.Parse(reader["cnt_stage"].ToString())
                    };
                    decimal mnogitel;
                    if (reader["formula"] != DBNull.Value)
                        counters.Formula = Decimal.TryParse(reader["formula"].ToString(), out mnogitel) ? mnogitel : 0;
                    else
                        counters.Formula = 1;
                    if (reader["dat_provnext"] != DBNull.Value)
                        counters.DatProv = ((DateTime)reader["dat_provnext"]).ToShortDateString();
                    else if (reader["dat_prov"] != DBNull.Value)
                    {
                        counters.DatProv = ((DateTime)reader["dat_prov"]).ToShortDateString();
                    }
                    counters.IsGkal = false;

                    if (reader["is_gkal"] != DBNull.Value)
                        if (reader["is_gkal"].ToString().Trim() == "1")
                            counters.IsGkal = true;
                    AddDomCounters(counters);
                }
                reader.Close();
                reader.Dispose();
                cmd.Dispose();
            }
            catch (Exception ex)
            {
                cmd.Dispose();
            }
        }

        public void FillCounters(String database, Int32 m, Int32 y, String nzp_kvar, String nzp_dom)
        {
            DateTime firstDayNextMonth = Convert.ToDateTime("01." + m + "." + y).AddMonths(1);

            string cmdText = @"drop table if exists t_serv";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                return;
            }

            cmdText = @"Create temp table t_serv(service char(100), nzp_serv integer, cnt_stage integer, nzp_cnttype integer, formula char(100), num_cnt char(100), 
                                        dat_uchet date, val_cnt decimal(13,2), service_small char(20), name_y char(255), dat_prov date, dat_provnext date, nzp_counter integer, dat_close date, 
                                        is_actual integer, dat_uchet2 date)";
            cmd = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                return;
            }

            cmdText = @"INSERT INTO t_serv SELECT service_name AS service,        cs.nzp_serv,        sc.cnt_stage,        cs.nzp_cnttype,        sc.formula,       
cs.num_cnt,        a.dat_uchet,        a.val_cnt,        service_small,        name_y,        cs.dat_prov,        
cs.dat_provnext, a.nzp_counter, a.dat_close, a.is_actual, a.dat_uchet    FROM  bill01_data.counters_spis cs          left outer join  bill01_data.counters a  on   
cs.nzp_counter = a.nzp_counter     LEFT OUTER JOIN  bill01_kernel.s_counttypes sc on   a.nzp_cnttype = sc.nzp_cnttype     LEFT OUTER JOIN 
bill01_kernel.services s on s.nzp_serv=cs.nzp_serv    LEFT JOIN bill01_data.prm_17 p ON p.is_actual=1        AND p.dat_s<= current_date       
AND p.dat_po>= current_date       AND a.nzp_counter=p.nzp and trim(p.val_prm)<>''       AND p.nzp_prm=974        LEFT JOIN bill01_kernel.res_y v 
ON p.val_prm::numeric=v.nzp_y        AND v.nzp_res=9990        WHERE  0=        (SELECT count(*)        FROM bill01_data.counters_spis d       
WHERE cs.nzp_counter=d.nzp_counter        AND d.is_actual = 1        AND d.dat_close IS NOT NULL AND d.dat_close <= current_date) and  cs.nzp =" + nzp_kvar +
 @" UNION ALL        SELECT service_name AS service,        cs1.nzp_serv,        sc.cnt_stage,        cs1.nzp_cnttype,        sc.formula,        
cs1.num_cnt,       cs.dat_uchet,       cs.val_cnt,   service_small,     name_y,        cs1.dat_prov,         cs1.dat_provnext,        cs.nzp_counter,       
a.dat_close,        cs.is_actual,        cs.dat_uchet    FROM bill01_data.counters_link cl     INNER JOIN bill01_data.counters_group cs on cs.nzp_counter =
cl.nzp_counter     INNER JOIN bill01_data.counters_spis cs1 on cs1.nzp_counter = cs.nzp_counter     left outer join  bill01_data.counters a  on  
cs.nzp_counter = a.nzp_counter     LEFT OUTER JOIN  bill01_kernel.s_counttypes sc on   a.nzp_cnttype = sc.nzp_cnttype   
LEFT OUTER JOIN  bill01_kernel.services s on s.nzp_serv=cs1.nzp_serv    LEFT JOIN bill01_data.prm_17 p ON p.is_actual=1        
AND p.dat_s<= current_date       AND p.dat_po>= current_date    AND a.nzp_counter=p.nzp and trim(p.val_prm)<>''       AND p.nzp_prm=974        
LEFT JOIN bill01_kernel.res_y v ON p.val_prm::numeric=v.nzp_y        AND v.nzp_res=9990       
WHERE  0=      (SELECT count(*)        FROM bill01_data.counters_spis d        WHERE cs.nzp_counter=d.nzp_counter        
AND d.is_actual = 1        AND d.dat_close IS NOT NULL AND d.dat_close <= current_date) and   cl.nzp_kvar = "+ nzp_kvar;
            cmd = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                return;
            }

            cmdText = @"SELECT service, nzp_serv, cnt_stage, nzp_cnttype, formula, num_cnt, dat_uchet, val_cnt, service_small, name_y, dat_prov, dat_provnext       
FROM t_serv t        WHERE dat_close IS NULL AND is_actual = 1            
AND (dat_uchet2=     (SELECT max(dat_uchet)   FROM bill01_data.counters c  WHERE t.nzp_counter = c.nzp_counter        AND c.dat_uchet <= '" + firstDayNextMonth.ToShortDateString() +
@"'        AND c.is_actual = 1) OR dat_uchet2=     (SELECT max(dat_uchet)   FROM bill01_data. counters_group c  WHERE t.nzp_counter = c.nzp_counter 
AND c.dat_uchet <= '" + firstDayNextMonth.ToShortDateString() + "'  AND c.is_actual = 1)) AND dat_uchet2>=Date('01.01."+(y-1)+"')   AND dat_uchet2<='"+ firstDayNextMonth.ToShortDateString() + "'";
            cmd = new NpgsqlCommand(cmdText, conn);

            decimal domBValueForHvsNaGvs = 0;
            //conn.Open();
            IDataReader reader = null;
            try
            {
                reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    var counters = new Counters
                    {
                        NzpServ = reader["nzp_serv"] != DBNull.Value ? Int32.Parse(reader["nzp_serv"].ToString()) : 0,
                        ServiceName = reader["service"] != DBNull.Value ? reader["service"].ToString() : "",
                        Value = reader["val_cnt"] != DBNull.Value ? Decimal.Parse(reader["val_cnt"].ToString()) : 0,
                        DatUchet = reader["dat_uchet"] != DBNull.Value ? (DateTime)reader["dat_uchet"] : DateTime.MinValue
                    };
                    counters.ValuePred = counters.Value;
                    counters.Place = reader["name_y"] != DBNull.Value ? reader["name_y"].ToString() : "";
                    counters.NumCounters = reader["num_cnt"] != DBNull.Value ? reader["num_cnt"].ToString().Trim() : "";
                    counters.DatUchetPred = counters.DatUchet;
                    counters.CntStage = reader["cnt_stage"] != DBNull.Value ? Int32.Parse(reader["cnt_stage"].ToString()) : 0;
                    decimal mnogitel;
                    counters.Formula = Decimal.TryParse(reader["formula"] != DBNull.Value ? reader["formula"].ToString() : "0", out mnogitel) ? mnogitel : 0;

                    if (reader["dat_provnext"] != DBNull.Value)
                        counters.DatProv = ((DateTime)reader["dat_provnext"]).ToShortDateString();
                    else if (reader["dat_prov"] != DBNull.Value)
                    {
                        counters.DatProv = ((DateTime)reader["dat_prov"]).ToShortDateString();
                    }
                    AddCounters(counters);
                }
                reader.Close();
                reader.Dispose();
                cmd.Dispose();
                //sw2.Close();
            }
            catch (Exception ex)
            {

                cmd.Dispose();
                return;
            }
        }

        public virtual void AddDomCounters(Counters aCounter)
        {
            var aCount = new Counters
            {
                NzpServ = aCounter.NzpServ,
                ServiceName = aCounter.ServiceName,
                Value = aCounter.Value,
                DatUchet = aCounter.DatUchet,
                ValuePred = aCounter.ValuePred,
                DatUchetPred = aCounter.DatUchetPred,
                NumCounters = aCounter.NumCounters,
                CntStage = aCounter.CntStage,
                DatProv = aCounter.DatProv,
                IsGkal = aCounter.IsGkal,
                Measure = aCounter.Measure
            };
            ListDomCounters.Add(aCount);
        }

        public virtual void AddCounters(Counters aCounter)
        {
            var aCount = new Counters
            {
                NzpServ = aCounter.NzpServ,
                ServiceName = aCounter.ServiceName,
                Value = aCounter.Value,
                DatUchet = aCounter.DatUchet,
                ValuePred = aCounter.ValuePred,
                DatUchetPred = aCounter.DatUchetPred,
                NumCounters = aCounter.NumCounters,
                CntStage = aCounter.CntStage,
                Place = aCounter.Place,
                DatProv = aCounter.DatProv,
                IsGkal = aCounter.IsGkal,
                Measure = aCounter.Measure
            };
            ListCounters.Add(aCount);
        }

        public void FillServNorm(String database, Int32 m, Int32 y, String nzp_kvar)
        {
            string cmdText = @"SELECT nzp_serv, rashod_norm, gil FROM bill01_charge_"+(y-2000).ToString("00")+".calc_gku_"+m.ToString("00")+
                " a  WHERE a.nzp_kvar= "+nzp_kvar+" AND a.stek = 3";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);

            decimal domBValueForHvsNaGvs = 0;
            IDataReader reader = null;
            try
            {
                reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    foreach (BaseServ t in ListServ)
                    {
                        if (t.Serv.NzpServ == Convert.ToInt32(reader["nzp_serv"]))
                        {

                            if ((t.Serv.NzpServ == 6) ||
                                (t.Serv.NzpServ == 7) ||
                                (t.Serv.NzpServ == 9))
                            {
                                if (Convert.ToInt32(reader["gil"]) > 0)
                                {
                                    try
                                    {
                                        t.Serv.Norma = Convert.ToDecimal(reader["rashod_norm"]) /
                                                   Convert.ToInt32(reader["gil"]);
                                    }
                                    catch
                                    {
                                        t.Serv.Norma = Convert.ToDecimal(reader["rashod_norm"].ToString().Replace(",",".")) /
                                                   Convert.ToInt32(reader["gil"]);
                                    }
                                    
                                }
                                if (t.Serv.NzpServ == 7) KanNormCalc = t.Serv.Norma;
                            }

                        }
                    }
                }
                reader.Close();
                reader.Dispose();
                cmd.Dispose();
            }
            catch
            {
                cmd.Dispose();
            }
        }

        public virtual void AddDomVolume(ServVolume aVolume)
        {

            int i = 0;
            bool findServ = false;


            while ((i < ListVolume.Count) & (findServ == false))
            {
                if (ListVolume[i].NzpServ == aVolume.NzpServ)
                {
                    ListVolume[i].DomArendatorsVolume = 0;
                    ListVolume[i].DomArendatorsVolume = aVolume.DomArendatorsVolume;
                    ListVolume[i].DomLiftVolume = aVolume.DomLiftVolume;
                    ListVolume[i].DomVolume = aVolume.DomVolume;
                    ListVolume[i].OdnDomVolume = aVolume.OdnDomVolume;
                    ListVolume[i].Kf307 = aVolume.Kf307;
                    ListVolume[i].AllLsVolume = aVolume.AllLsVolume;
                    findServ = true;
                }
                i++;
            }

            if (findServ == false) ListVolume.Add(aVolume);
        }

        public void FillInfo(String database, Int32 m, Int32 y, String nzp_kvar)
        {
            decimal sumKanNorm = 0;
            HvsNorm = 0;
            GvsNorm = 0;
            var dat = new DateTime(y, m, 1);
            string cmdText = @"SELECT s.service_name as service, a.nzp_serv, (dlt_reval+valm) as rashod, a.rashod_norm , a.dop87 as odn,         
a.is_device,squ,  a.gil, a.valm, a.rashod as rashod_all, a.rsh1, a.rsh2,  a.rash_norm_one, a.tarif  
FROM bill01_charge_"+(y-2000).ToString("00")+".calc_gku_"+m.ToString("00")+@" a, bill01_kernel.services s  
WHERE a.nzp_serv=s.nzp_serv AND a.nzp_kvar="+ nzp_kvar + " and a.nzp_serv<>500 AND a.stek = 3 AND a.dat_s >= '"+ dat.ToShortDateString() + 
"'  AND a.dat_po < '"+ dat.AddMonths(1).ToShortDateString() + "'  ORDER BY 1";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);
            int steps = 0;
            IDataReader reader = null;
            try
            {

                reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    var servVolume = new ServVolume
                    {
                        ServiceName = reader["service"].ToString(),
                        NzpServ = Int32.Parse(reader["nzp_serv"].ToString())
                    };


                    decimal rashod = reader["rashod"] != null
                        ? Decimal.Parse(reader["rashod"].ToString())
                        : 0;
                    steps = 1;
                    decimal rashodOdn = reader["odn"] != null
                        ? Decimal.Parse(reader["odn"].ToString())
                        : 0;
                    steps = 2;
                    decimal rashNormOne =
                        reader["rash_norm_one"] != null
                            ? Decimal.Parse(reader["rash_norm_one"].ToString())
                            : 0;
                    steps = 3;
                    decimal rsh2 = reader["rsh2"] != null
                        ? Decimal.Parse(reader["rsh2"].ToString())
                        : 0;
                    steps = 4;
                    decimal rashodAll = reader["rashod_all"] != null
                        ? Decimal.Parse(reader["rashod_all"].ToString())
                        : 0;
                    steps = 5;
                    decimal tarif = reader["tarif"] != null
                        ? Decimal.Parse(reader["tarif"].ToString())
                        : 0;
                    steps = 6;

                    int gil = reader["gil"] != null
                        ? (int)Decimal.Parse(reader["gil"].ToString())
                        : 0;
                    steps = 7;
                    decimal squ = reader["squ"] != null
                        ? Decimal.Parse(reader["squ"].ToString())
                        : 0;
                    steps = 8;
                    decimal rashodNorm = reader["rashod_norm"] != null
                        ? Decimal.Parse(reader["rashod_norm"].ToString())
                        : 0;
                    steps = 9;
                    int isDevice = reader["is_device"] != null
                        ? (int)Decimal.Parse(reader["is_device"].ToString())
                        : 0;
                    steps = 10;
                    if ((tarif <= 0.0001m) && (rashodOdn == 0m))
                    {
                        rashod = 0;
                        rashodOdn = 0;
                    }

                    if ((servVolume.NzpServ == 6) ||
                             (servVolume.NzpServ == 7) ||
                             (servVolume.NzpServ == 9) ||
                             (servVolume.NzpServ == 25) ||
                             (servVolume.NzpServ == 210) ||
                             (servVolume.NzpServ == 10) ||
                             (servVolume.NzpServ == 14)
                        )
                    {
                        if (gil > 0)
                        {
                            servVolume.NormaVolume = rashodNorm / gil;
                        }
                        else
                        {
                            servVolume.NormaVolume = rashNormOne;
                        }
                    }
                    servVolume.IsPu = isDevice;


                    if ((rashodAll <= 0m) & (rashod > 0m))
                    {
                        if ((rashodOdn < 0m) & (rashodOdn + rashod < -0.00001m))
                        {
                            rashodOdn = -rashod;
                        }
                    }
                    else
                        if ((rashodAll <= 0m) & (rashod <= 0m) & (rashodOdn < 0m))
                    {
                        rashodOdn = 0;
                    }


                    servVolume.NormaFullVolume = rashod;
                    if (servVolume.IsPu != 0)
                    {
                        servVolume.PUVolume = rashod;
                        servVolume.OdnFlatPuVolume = rashodOdn;

                    }
                    else
                    {
                        servVolume.OdnFlatNormVolume = rashodOdn;
                    }


                    if ((servVolume.NzpServ == 6) || (servVolume.NzpServ == 9))
                    {
                        sumKanNorm += servVolume.NormaVolume;
                    }
                    if (servVolume.NzpServ == 6) HvsNorm = servVolume.NormaVolume;
                    if (servVolume.NzpServ == 9)
                    {
                        GvsNorm = servVolume.NormaVolume;
                    }

                    if ((servVolume.NzpServ == 8) & (rashod == 0))
                        servVolume.NormaFullVolume = rsh2;


                    CalcSquare = squ;
                    FullSquare = (CalcSquare < FullSquare
                        ? FullSquare
                        : CalcSquare);
                    AddVolume(servVolume);
                }

                reader.Close();

                reader.Dispose();

                cmd.Dispose();


            }
            catch (Exception ex)
            {

                cmd.Dispose();
            }


            if (sumKanNorm > 0.001m)
            {
                var servVolume = new ServVolume
                {
                    NormaVolume = sumKanNorm,
                    PUVolume = 0,
                    OdnFlatNormVolume = 0,
                    OdnFlatPuVolume = 0,
                    ServiceName = "Водоотведение",
                    NzpServ = 7
                };


                AddVolume(servVolume);
            }
        }

        public struct ServReval
        {
            public int NzpServ;
            public string ServiceName;
            public string Reason;
            public string ReasonPeriod;
            public decimal SumReval;
            public decimal SumGilReval;
            public decimal CReval;
        }

        public virtual void AddReval(SumServ aServ)
        {
            ServReval aReval;
            bool findServ = false;


            for (int i = 0; i < ListReval.Count; i++)
            {
                if (ListReval[i].NzpServ == aServ.NzpServ)
                {
                    aReval = ListReval[i];
                    aReval.SumReval += aServ.Reval + aServ.RealCharge;
                    aReval.CReval += aServ.CReval;
                    aReval.SumGilReval += aServ.RevalGil;
                    ListReval[i] = aReval;
                    findServ = true;
                }
            }

            if (findServ == false)
            {
                aReval = new ServReval
                {
                    NzpServ = aServ.NzpServ,
                    ServiceName = aServ.NameServ,
                    SumReval = aServ.Reval + aServ.RealCharge,
                    CReval = aServ.CReval,
                    SumGilReval = aServ.RevalGil
                };

                ListReval.Add(aReval);
            }
        }

        public virtual void AddServ(BaseServ aServ)
        {
            if (aServ.Empty())
                return;
            if(ListKommServ != null)
            {
                foreach (BaseServ baseServ in ListKommServ)
                {
                    if (baseServ.Serv.NzpServ == aServ.Serv.NzpServ)
                        aServ.KommServ = true;
                }
            }
            
            SummaryServ.AddSum(aServ.Serv);
            if (Math.Abs(aServ.Serv.Reval) > 0.001m || Math.Abs(aServ.Serv.RealCharge) > 0.001m)
                AddReval(aServ.Serv);
            BaseServ mainServBySlave = CUnionServ.GetMainServBySlave(aServ.Serv.NzpServ);
            if (mainServBySlave == null || aServ.Serv.NzpServ == 14)
            {
                bool flag = false;
                foreach (BaseServ baseServ in ListServ)
                {
                    if (baseServ.Serv.NzpServ == aServ.Serv.NzpServ)
                    {
                        baseServ.AddSum(aServ.Serv);
                        if (baseServ.Serv.NameSupp == "")
                        {
                            baseServ.Serv.NameSupp = aServ.Serv.NameSupp;
                            baseServ.Serv.SuppRekv = aServ.Serv.SuppRekv;
                        }
                        else if (aServ.Serv.Tarif > new Decimal(0))
                        {
                            baseServ.Serv.NameSupp = aServ.Serv.NameSupp;
                            baseServ.Serv.SuppRekv = aServ.Serv.SuppRekv;
                            if (!aServ.Serv.IsOdn)
                                baseServ.Serv.NameServ = aServ.Serv.NameServ;
                        }
                        flag = true;
                    }
                }
                if (flag)
                    return;
                ListServ.Add(aServ);
            }
            else
            {
                bool flag = false;
                foreach (BaseServ baseServ in ListServ)
                {
                    if (baseServ.Serv.NzpServ == mainServBySlave.Serv.NzpServ)
                    {
                        if (baseServ.Serv.NzpServ == 25 && aServ.Serv.NzpServ == 515 && (baseServ.Serv.Tarif == 2.45m || baseServ.Serv.Tarif == 0m))
                            baseServ.Serv.NameServ = "Электроэнергия день";
                        if (aServ.Serv.IsOdn)
                            aServ.Serv.CanAddTarif = false;
                        if (aServ.Serv.Tarif > 0m)
                        {
                            baseServ.Serv.NameSupp = aServ.Serv.NameSupp;
                            baseServ.Serv.SuppRekv = aServ.Serv.SuppRekv;
                            if (!aServ.Serv.IsOdn)
                                baseServ.Serv.NameServ = aServ.Serv.NameServ;
                        }
                        baseServ.AddSum(aServ.Serv);
                        flag = true;
                    }
                }
                if (!flag)
                {

                    BaseServ baseServ = (BaseServ)mainServBySlave.Clone();
                    baseServ.KommServ = aServ.KommServ;
                    if (aServ.Serv.IsOdn)
                        aServ.Serv.CanAddTarif = false;
                    if (aServ.Serv.Tarif > new Decimal(0))
                    {
                        baseServ.Serv.NameSupp = aServ.Serv.NameSupp;
                        baseServ.Serv.SuppRekv = aServ.Serv.SuppRekv;
                        if (!aServ.Serv.IsOdn)
                        {
                            baseServ.Serv.NameServ = aServ.Serv.NameServ;
                            baseServ.Serv.IsDevice = aServ.Serv.IsDevice;
                        }
                    }
                    if (aServ.Serv.NzpServ == 515)
                        baseServ.Serv.NameServ = "Электроэнергия день";
                    baseServ.Serv.Measure = aServ.Serv.Measure;
                    baseServ.AddSum(aServ.Serv);
                    ListServ.Add(baseServ);
                }
            }
        }

        private void LoadFormulList(String database)
        {
            if (_formulList == null) _formulList = new Dictionary<int, string>();
            else _formulList.Clear();
            string s = " SELECT nzp_frm, a.nzp_measure, measure   FROM fbill_kernel.formuls a, fbill_kernel.s_measure b WHERE a.nzp_measure = b.nzp_measure";
            NpgsqlCommand cmd = new NpgsqlCommand(s, conn);
            IDataReader reader = null;
            try
            {
                reader = cmd.ExecuteReader();
                while (reader.Read())
                {


                    if (reader["nzp_frm"] != DBNull.Value)
                    {
                        _formulList.Add(Convert.ToInt32(reader["nzp_frm"]),
                            reader["nzp_measure"].ToString().Trim() + "=" +
                            reader["measure"].ToString().Trim());
                    }
                }
            }
            catch (Exception e)
            {
                cmd.Dispose();
            }
        }

        private void GetMeasureByFrm(int nzpFrm, ref string edIzmer, ref int nzpMeasure)
        {
            if (_formulList != null && nzpFrm != 0)
            {
                var formula = from cust in _formulList
                              where cust.Key == nzpFrm
                              select cust.Value;

                foreach (string value in formula)
                {
                    nzpMeasure = Convert.ToInt32(value.Substring(0, value.IndexOf("=", StringComparison.Ordinal)));
                    if (value.IndexOf("=", StringComparison.Ordinal) > -1)
                    {
                        edIzmer = value.Substring(value.IndexOf("=", StringComparison.Ordinal) + 1, value.Length -
                            value.IndexOf("=", StringComparison.Ordinal) - 1);
                    }

                }

            }
        }

        private void DropTable(String tableName, String database)
        {        
            string cmdText = @"drop table if exists " + tableName;           
            NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
        }

        private void CreateIndex(String tableName, String database, String indexName, String indexColumn)
        {
            string cmdText = @"create index "+ indexName + " on "+ tableName + "("+ indexColumn + ")";
            NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
        }

        private void Analyze(String tableName, String database)
        {
            string cmdText = @"analyze " + tableName;
            NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
        }

        private void CreateFselKvar(String database)
        {
            string cmdText = @"Create temp table fsel_kvar (  nzp_kvar integer,  num_ls integer,  pkod numeric(13,0),  nzp_dom integer,  nzp_ul integer,  
                                typek integer default 0,  fio char(100),  ulica char(100),  ndom char(20),  idom integer,  ikvar integer,  nkvar char(20),  
                                nkvar_n char(10),  nzp_geu integer default 0, nzp_area integer default 0,  uch integer default 0,  pref char(10) )";
            NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
        }

        private void FillFselKvar(String database, String whereDom)
        {
            string cmdText = @"INSERT INTO fsel_kvar ( nzp_kvar, num_ls, pkod, nzp_dom, nzp_ul, typek, fio, ulica, ndom, idom, ikvar, nkvar,nkvar_n, nzp_geu, 
                                nzp_area, uch, pref)  
                              SELECT k.nzp_kvar, k.num_ls, k.pkod, k.nzp_dom, d.nzp_ul, k.typek, k.fio, (trim(coalesce(s.ulicareg,''))||' '||s.ulica) as ulica,      
                                trim(d.ndom)||' '||(case when trim(coalesce(d.nkor,'')) not in ('', '-', '*') then 'корп.'||trim(d.nkor) else '' end) as ndom,        
                                d.idom, k.ikvar, trim(coalesce(k.nkvar,'')) as nkvar, trim(coalesce(k.nkvar_n,'')) as nkvar_n, k.nzp_geu, k.nzp_area, 0 as uch,
                                k.pref  FROM fbill_data.kvar k , fbill_data.dom d, fbill_data.s_ulica s  
                                WHERE k.num_ls>0 AND k.nzp_dom=d.nzp_dom AND d.nzp_ul=s.nzp_ul " +
                                " and k.nzp_dom  in (" + whereDom + ")" + 
                                " AND k.pref = 'bill01'";
            NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                int i = cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
        }

        private void FillFselKvar(String database, Int32 numLs)
        {
            string cmdText = @"INSERT INTO fsel_kvar ( nzp_kvar, num_ls, pkod, nzp_dom, nzp_ul, typek, fio, ulica, ndom, idom, ikvar, nkvar,nkvar_n, nzp_geu, 
                                nzp_area, uch, pref)  
                              SELECT k.nzp_kvar, k.num_ls, k.pkod, k.nzp_dom, d.nzp_ul, k.typek, k.fio, (trim(coalesce(s.ulicareg,''))||' '||s.ulica) as ulica,      
                                trim(d.ndom)||' '||(case when trim(coalesce(d.nkor,'')) not in ('', '-', '*') then 'корп.'||trim(d.nkor) else '' end) as ndom,        
                                d.idom, k.ikvar, trim(coalesce(k.nkvar,'')) as nkvar, trim(coalesce(k.nkvar_n,'')) as nkvar_n, k.nzp_geu, k.nzp_area, 0 as uch,
                                k.pref  FROM fbill_data.kvar k , fbill_data.dom d, fbill_data.s_ulica s  
                                WHERE k.num_ls>0 AND k.nzp_dom=d.nzp_dom AND d.nzp_ul=s.nzp_ul " +
                                " and k.num_ls =  " + numLs + 
                                " AND k.pref = 'bill01'";
            NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                int i = cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
        }

        private void CreateTFkvarPrm(String database)
        {
            string cmdText = @"Create temp table t_fkvar_prm ( nzp_kvar integer,  nzp_dom integer,  num_ls char(10),  typek char(10),  nzp_dom_base integer,  
                                indecs integer,  is_open integer default 0,  is_print integer default 1,  is_komm integer default 0,  fio char(40),  et integer,  
                                count_gil integer,  count_gil_time integer,  count_domgil integer,  count_gilp integer,  count_gil_time_epd integer,  
                                is_paspgil integer default 0,  pl_kvar numeric(14,4),  pl_kvar_gil numeric(14,4),  pl_dom numeric(14,4),  pl_mop numeric(14,4),  
                                otop_norm_k numeric(14,7),  otop_norm_i numeric(14,7),  gvs_norm_gkal numeric(14,7),  hasElDpu integer,  hasHvsDpu integer,  
                                hasGvsDpu integer,  hasGazDpu integer,  hasOtopDpu integer,  privat integer,  open_vodozabor integer)";
            NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
        }

        private void FillTFkvarPrm(String database)
        {
            string cmdText = @"insert into t_fkvar_prm(nzp_kvar, num_ls, nzp_dom, typek) 
                                SELECT nzp_kvar, num_ls, nzp_dom,  case when typek = 3 then 'нежилого' else 'жилого' end as typek FROM fsel_kvar";
            NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                int i = cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
        }

        private void CreateTFreasonReval(String database)
        {
            string cmdText = @"Create temp table t_freasonReval ( nzp_kvar integer,  nzp_serv integer,  source char(250),  new_counters integer,  kod_reval integer, 
                                dat_plomb char(10),  num_cnt char(20),  nedop integer,  izm_comment char(100),  counters integer)";
            NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
        }

        private void CreateTFVolume(String database)
        {
            string cmdText = @"Create temp table t_fVolume ( nzp_kvar integer,  nzp_serv integer,  cnt_stage integer,  hvscnt2 integer default 0,  
                                hvscnt3 integer default 0,  gvscnt2 integer default 0,  gvscnt3 integer default 0,  pu numeric(14,4),  norm numeric(14,4),  
                                norm_full numeric(14,4),  pu_odn numeric(14,4),  norm_odn numeric(14,4),  koef numeric(14,7),   counters integer)";
            NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
        }

        private void CreateTFPerekidka(String database)
        {
            string cmdText = @"Create temp table t_fPerekidka ( nzp_kvar integer,  nzp_serv integer,  comment char(100),  sum_rcl Decimal(14,4))";
            NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
        }

        private void UpdateTFkvarPrm(String database, Int32 m, Int32 y, Int16 step)
        {
            switch(step)
            {
                #region step1
                case 1:
                    {
                        string cmdText = @"UPDATE t_fkvar_prm SET nzp_dom_base = (SELECT a.nzp_dom_base  FROM bill01_data.link_dom_lit a WHERE t_fkvar_prm.nzp_dom=a.nzp_dom) 
                            WHERE 0 < (SELECT count(*) FROM bill01_data.link_dom_lit k WHERE k.nzp_dom =t_fkvar_prm.nzp_dom)";
                        NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);

                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }

                        cmdText = @"update t_fkvar_prm SET indecs = (SELECT max(a.val_prm::numeric) FROM bill01_data.prm_2 a 
                        WHERE t_fkvar_prm.nzp_dom=a.nzp AND a.nzp_prm=68 AND a.is_actual=1 AND a.dat_s<='01." + m + "." + y +
                                    "'AND a.dat_po>='01." + m + "." + y + @"') WHERE 0 < (SELECT count(*) FROM bill01_data.prm_2 k 
                        WHERE k.nzp = t_fkvar_prm.nzp_dom AND k.nzp_prm = 68                    AND k.is_actual = 1)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"UPDATE t_fkvar_prm SET count_gil = (SELECT max(val_prm::numeric) as count_gil FROM bill01_data.prm_1 p WHERE p.nzp_prm=5                
                        AND p.is_actual=1 AND p.dat_s<='01." + m + "." + y + "' AND p.dat_po>='01." + m + "." + y + @"' AND p.nzp = t_fkvar_prm.nzp_kvar) 
                        WHERE 0 < (SELECT count(*) FROM bill01_data.kvar k   WHERE k.nzp_kvar = t_fkvar_prm.nzp_kvar)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"UPDATE t_fkvar_prm SET count_gil_time = (SELECT max(val_prm::numeric) as count_gil FROM bill01_data.prm_1 p WHERE p.nzp_prm=131              
                        AND p.is_actual=1 AND p.dat_s<='01." + m + "." + y + "' AND p.dat_po>='01." + m + "." + y + @"' AND p.nzp = t_fkvar_prm.nzp_kvar) 
                        WHERE 0 < (SELECT count(*)  FROM bill01_data.kvar k  WHERE k.nzp_kvar = t_fkvar_prm.nzp_kvar)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"update t_fkvar_prm SET count_gilp = (SELECT max(p.val_prm::numeric) as count_gil         FROM bill01_data.prm_1 p         
WHERE p.nzp_prm = 1010270                AND p.is_actual = 1                AND p.dat_s <= '01." + m + "." + y + "'   AND p.dat_po >= '01." + m + "." + y + @"'              
AND p.nzp = nzp_kvar) WHERE 0 < (SELECT count(*)             FROM bill01_data.kvar k             WHERE k.nzp_kvar = t_fkvar_prm.nzp_kvar)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"update t_fkvar_prm SET count_gil_time_epd = (SELECT max(p.val_prm::numeric) as count_gil         FROM bill01_data.prm_1 p         
WHERE p.nzp_prm = 1377                AND p.is_actual = 1                AND p.dat_s <= '01." + m + "." + y + "'               AND p.dat_po >= '01." + m + "." + y + @"'         
AND p.nzp = nzp_kvar) WHERE 0 < (SELECT count(*)             FROM bill01_data.kvar k             WHERE k.nzp_kvar = t_fkvar_prm.nzp_kvar)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"update t_fkvar_prm SET pl_kvar = (SELECT max(Replace(p.val_prm, ',', '.')::numeric) as pl_kvar        FROM bill01_data.prm_1 p         
WHERE p.nzp_prm = 4                AND p.is_actual = 1                AND p.dat_s <= '01." + m + "." + y + "'               AND p.dat_po >= '01." + m + "." + y + @"'               
AND p.nzp = t_fkvar_prm.nzp_kvar) WHERE 0 < (SELECT count(*)             FROM bill01_data.kvar k             WHERE k.nzp_kvar = t_fkvar_prm.nzp_kvar)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"UPDATE t_fkvar_prm SET pl_kvar_gil = (SELECT max(Replace(p.val_prm, ',', '.')::numeric) as pl_kvar        FROM bill01_data.prm_1 p        
WHERE p.nzp_prm = 6                AND p.is_actual = 1                AND p.dat_s <= '01." + m + "." + y + "'               AND p.dat_po >= '01." + m + "." + y + @"'               
AND p.nzp = t_fkvar_prm.nzp_kvar) WHERE 0 < (SELECT count(*)             FROM bill01_data.kvar k             WHERE k.nzp_kvar = t_fkvar_prm.nzp_kvar)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"UPDATE t_fkvar_prm SET privat = 1  WHERE 0 < (SELECT count(*) as privat           FROM bill01_data.prm_1 p           WHERE p.nzp_prm = 8   
AND p.is_actual = 1                  AND p.dat_s <= '01." + m + "." + y + "'                 AND p.val_prm = '1'                  AND p.dat_po >= '01." + m + "." + y + @"'     
AND p.nzp = t_fkvar_prm.nzp_kvar) AND 0 < (SELECT count(*)             FROM bill01_data.kvar k             WHERE k.nzp_kvar = t_fkvar_prm.nzp_kvar)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"UPDATE t_fkvar_prm SET is_komm = 1  WHERE 0 < (SELECT count(*) as komm           FROM bill01_data.prm_1 p           WHERE p.nzp_prm = 3    
AND p.is_actual = 1                  AND p.dat_s <= '01." + m + "." + y + "'                 AND p.val_prm = '2'                  AND p.dat_po >= '01." + m + "." + y + @"'     
AND p.nzp = t_fkvar_prm.nzp_kvar)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"UPDATE t_fkvar_prm SET is_print = 0  WHERE 0 < (SELECT count(*)             FROM bill01_data.prm_1 p           WHERE p.nzp_prm = 23        
AND p.is_actual = 1                  AND p.dat_s <= '01." + m + "." + y + @"'                 AND p.dat_po >= '01." + m + "." + y + @"' 
AND p.nzp = t_fkvar_prm.nzp_kvar)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"update t_fkvar_prm SET et = (SELECT max(p.val_prm::numeric) as et        FROM bill01_data.prm_1 p         WHERE p.nzp_prm = 2               
AND p.is_actual = 1                AND p.dat_s <= '01." + m + "." + y + "'               AND p.dat_po >= '01." + m + "." + y + @"'        
AND p.nzp = t_fkvar_prm.nzp_kvar) WHERE 0 < (SELECT count(*)             FROM bill01_data.kvar k             WHERE k.nzp_kvar = t_fkvar_prm.nzp_kvar)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"UPDATE t_fkvar_prm SET is_paspgil = 1  WHERE 0 < (SELECT count(*) as paspgil           FROM bill01_data.prm_1 p           
WHERE p.nzp_prm = 130                  AND p.is_actual = 1                  AND p.dat_s <= '01." + m + "." + y + @"'                 AND p.val_prm = '1'                  
AND p.dat_po >= '01." + m + "." + y + @"'                 AND p.nzp = t_fkvar_prm.nzp_kvar) AND 0 < (SELECT count(*)             FROM bill01_data.kvar k            
WHERE k.nzp_kvar = t_fkvar_prm.nzp_kvar)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"UPDATE t_fkvar_prm SET fio = (SELECT max(p.val_prm)         FROM bill01_data.prm_3 p         WHERE p.nzp_prm = 46                
AND p.is_actual = 1                AND p.dat_s <= '01." + m + "." + y + "'               AND p.dat_po >= '01." + m + "." + y + @"'               AND p.nzp = t_fkvar_prm.nzp_kvar) 
WHERE 0 < (SELECT count(*)             FROM bill01_data.kvar k             WHERE k.nzp_kvar = t_fkvar_prm.nzp_kvar)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"UPDATE t_fkvar_prm SET is_open = (SELECT max(1)         FROM bill01_data.prm_3 p         WHERE p.nzp_prm = 51                
AND p.is_actual = 1                AND p.dat_s <= '01." + m + "." + y + "'               AND p.val_prm = '1'               AND p.dat_po >= '01." + m + "." + y + @"' 
AND p.nzp = t_fkvar_prm.nzp_kvar) WHERE 0 < (SELECT count(*)           FROM bill01_data.kvar k           WHERE k.nzp_kvar = t_fkvar_prm.nzp_kvar)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"UPDATE t_fkvar_prm SET pl_dom = (SELECT max(val_prm::numeric)         FROM bill01_data.prm_2 p         WHERE p.nzp_prm = 40               
AND p.is_actual = 1                AND p.dat_s <= '01." + m + "." + y + "'               AND p.dat_po >= '01." + m + "." + y + @"'               AND p.nzp = t_fkvar_prm.nzp_dom) 
WHERE 0 < (SELECT count(*)           FROM bill01_data.kvar k           WHERE k.nzp_kvar = t_fkvar_prm.nzp_kvar)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"UPDATE t_fkvar_prm SET pl_dom = (SELECT sum(p.val_prm::numeric)         FROM bill01_data.prm_2 p, bill01_data.link_dom_lit d         
WHERE p.nzp_prm = 40                AND p.is_actual = 1                AND p.dat_s <= '01." + m + "." + y + @"'               AND p.dat_po >= '01." + m + "." + y + @"'   
AND p.nzp = d.nzp_dom                AND d.nzp_dom_base = t_fkvar_prm.nzp_dom_base) WHERE 0 < (SELECT count(*)           FROM bill01_data.kvar k          
WHERE k.nzp_kvar = t_fkvar_prm.nzp_kvar )                  AND t_fkvar_prm.nzp_dom_base is not null";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"UPDATE t_fkvar_prm SET pl_mop = (SELECT max(p.val_prm::numeric)         FROM bill01_data.prm_2 p         WHERE p.nzp_prm = 2049      
AND p.is_actual = 1                AND p.dat_s <= '01." + m + "." + y + "'               AND p.dat_po >= '01." + m + "." + y + @"'               AND p.nzp = t_fkvar_prm.nzp_dom) 
WHERE 0 < (SELECT count(*)           FROM bill01_data.kvar k           WHERE k.nzp_kvar = t_fkvar_prm.nzp_kvar)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"UPDATE t_fkvar_prm SET pl_mop = (SELECT sum(p.val_prm::numeric)         FROM bill01_data.prm_2 p, bill01_data.link_dom_lit d         
WHERE p.nzp_prm = 2049                AND p.is_actual = 1                AND p.dat_s <= '01." + m + "." + y + @"'               
AND p.nzp = d.nzp_dom                AND d.nzp_dom_base = t_fkvar_prm.nzp_dom_base                AND p.dat_po >= '01." + m + "." + y + @"'
AND p.nzp = d.nzp_dom) WHERE 0 < (SELECT count(*)           FROM bill01_data.kvar k           WHERE k.nzp_kvar = t_fkvar_prm.nzp_kvar)       
AND t_fkvar_prm.nzp_dom_base is not null";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        break;
                    }
                #endregion
                case 2:
                    {
                        string cmdText = @"UPDATE t_fkvar_prm SET count_domgil =(SELECT sum(t_12.count_gil)         FROM t_12        
WHERE t_12.nzp_dom = t_fkvar_prm.nzp_dom) WHERE 0<(SELECT count(*)           FROM t_12 k           WHERE k.nzp_dom = t_fkvar_prm.nzp_dom)";
                        NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);

                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        break;
                    }
                #region step 3
                case 3:
                    {
                        string cmdText = @"UPDATE t_fkvar_prm SET otop_norm_i =(        SELECT max(p.val_prm::numeric)         FROM bill01_data.prm_2 p         
WHERE p.nzp_prm = 723               AND p.is_actual = 1               AND p.dat_s <= '01."+m+"."+y+"'              AND p.dat_po >= '01."+m+"."+y+@"'
AND p.nzp = t_fkvar_prm.nzp_dom) WHERE 0<(SELECT count(*)           FROM bill01_data.kvar k           WHERE k.nzp_kvar =t_fkvar_prm.nzp_kvar)";
                        NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);

                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"UPDATE t_fkvar_prm SET otop_norm_k = (SELECT max(p.val_prm::numeric)         FROM bill01_data.prm_2 p        
WHERE p.nzp_prm = 2074                AND p.is_actual = 1                AND p.dat_s <= '01." + m + "." + y + "'               AND p.dat_po >= '01." + m + "." + y + @"'
AND p.nzp = t_fkvar_prm.nzp_dom) WHERE 0 < (SELECT count(*)           FROM bill01_data.kvar k           WHERE k.nzp_kvar = t_fkvar_prm.nzp_kvar)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }

                        cmdText = @"UPDATE t_fkvar_prm SET gvs_norm_gkal = (SELECT max(p.val_prm::numeric)         FROM bill01_data.prm_2 p         
WHERE p.nzp_prm = 436                AND p.is_actual = 1                AND p.dat_s <= '01." + m + "." + y + "'               AND p.dat_po >= '01." + m + "." + y + @"'             
AND p.nzp = t_fkvar_prm.nzp_dom) WHERE 0 < (SELECT count(*)           FROM bill01_data.kvar k           WHERE k.nzp_kvar = t_fkvar_prm.nzp_kvar)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }
                        cmdText = @"UPDATE t_fkvar_prm SET open_vodozabor = (SELECT max(coalesce(val_prm::numeric, 0))         FROM bill01_data.prm_2 p        
WHERE p.nzp_prm = 35                AND p.is_actual = 1                AND p.dat_s <= '01." + m + "." + y + "'               AND p.dat_po >= '01." + m + "." + y + @"'              
AND p.nzp = t_fkvar_prm.nzp_dom) WHERE 0 < (SELECT count(*)           FROM bill01_data.kvar k           WHERE k.nzp_kvar = t_fkvar_prm.nzp_kvar)";
                        cmd1 = new NpgsqlCommand(cmdText, conn);
                        try
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {

                        }

                        break;
                    }
                    #endregion
            }
        }

        private void CreateT12(String database)
        {
            string cmdText = @"CREATE TEMP TABLE t_12 (  nzp_dom integer,  count_gil integer)";
            NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
        }

        private void FillT12(String database, Int32 m, Int32 y)
        {
            string cmdText = @"INSERT INTO t_12(nzp_dom, count_gil)  SELECT nzp_dom, sum(cnt2-val5+val3) as count_gil  FROM bill01_charge_"+(y-2000).ToString("00")
                +@".gil_"+m.ToString("00")+@" a WHERE exists (SELECT 1 FROM t_fkvar_prm b   WHERE a.nzp_dom=b.nzp_dom)       AND stek=3  GROUP BY 1";
            NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }

            cmdText = @"UPDATE t_12 SET count_gil = coalesce(( SELECT sum(a.cnt2 - a.val5 + a.val3) as count_gil  FROM bill01_charge_" + (y - 2000).ToString("00")
                + @".gil_" + m.ToString("00") + @" a WHERE exists (SELECT 1 FROM t_fkvar_prm d, bill01_data.link_dom_lit k  WHERE a.nzp_dom=k.nzp_dom     
                AND d.nzp_dom_base=k.nzp_dom_base  AND d.nzp_dom_base is not null)),0)  WHERE exists (SELECT  1   FROM bill01_data.link_dom_lit t               
                WHERE t.nzp_dom=t_12.nzp_dom)";
            cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
        }

        private void FillTFreasonReval(String database, Int32 m, Int32 y)
        {
            string cmdText = @"INSERT into t_freasonReval(nzp_serv, nzp_kvar, source, izm_comment) SELECT a.nzp_serv, a.nzp_kvar,'Недопоставка ', 
'Недоставка с ' || TO_CHAR(a.dat_s, 'DD.mm.yyyy') || ' по ' || TO_CHAR(a.dat_po, 'DD.mm.yyyy') FROM bill01_data.nedop_kvar a, fsel_kvar b 
WHERE a.nzp_kvar = b.nzp_kvar AND month_calc = '" + y + "-" + m + "-1'::timestamp  AND is_actual = 1   GROUP BY 1,2,3,4";
            NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }

            cmdText = @"INSERT into t_freasonReval(nzp_serv, nzp_kvar, source) SELECT a.nzp_serv, a.nzp_kvar,'Cчетчики'  
FROM bill01_data.counters a, fsel_kvar k WHERE a.nzp_kvar = k.nzp_kvar AND month_calc = MDY(" + m + ", 01, " + y +
")  AND is_actual<> 100 AND dat_uchet< MDY(" + m + ", 01, " + y + ") GROUP BY 1,2,3";
            cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
            cmdText = @"INSERT into t_freasonReval(nzp_serv, nzp_kvar, source, num_cnt, dat_plomb) SELECT a.nzp_serv, k.nzp_kvar,'Новые счетчики', a.num_cnt, 
p.val_prm FROM  bill01_data.counters_spis a, bill01_data.prm_17 p, fsel_kvar k WHERE a.nzp_type = 3 AND a.nzp = k.nzp_kvar AND p.month_calc = MDY(" + m + ", 01, " + y +
")  AND a.is_actual <> 100 AND p.is_actual <> 100 AND p.nzp_prm = 2027 AND a.nzp_counter = p.nzp GROUP BY 1,2,3,4,5";
            cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
            cmdText = @"insert into t_freasonReval(nzp_serv, nzp_kvar, source, izm_comment) SELECT a.nzp_serv, b.nzp_kvar, 'Перекидки',c.comment 
FROM  bill01_charge_" + (y - 2000).ToString("00") + @".perekidka a, fsel_kvar b, fbill_data.document_base c  
WHERE a.nzp_kvar = b.nzp_kvar  AND a.nzp_doc_base = c.nzp_doc_base AND month_ = " + m + " AND c.comment != 'Выравнивание сальдо' GROUP BY 1,2,3,4";
            cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
            cmdText = @"insert into t_freasonReval(nzp_serv, nzp_kvar, source, izm_comment, kod_reval) 
SELECT a.nzp_serv, b.nzp_kvar, 'Must_calc',dat_s || '-' || dat_po, kod1 FROM  bill01_data.must_calc a, fsel_kvar b WHERE a.nzp_kvar = b.nzp_kvar  
AND nzp_serv> 1          AND month_ = " + m + "        AND year_ = " + y;
            cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
        }

        private void FillTFPerekidka(String database, Int32 m, Int32 y)
        {
            string cmdText = @"INSERT into t_fPerekidka(nzp_kvar, nzp_serv, sum_rcl, comment)  
SELECT a.nzp_kvar, a.nzp_serv, sum(a.sum_rcl) as sum_rcl, 'qqqqqqq'  FROM  bill01_charge_"+(y-2000).ToString("00")+@".perekidka  a, fsel_kvar b 
WHERE a.nzp_kvar=b.nzp_kvar AND type_rcl = 63          AND month_="+m+" GROUP BY 1,2";
            NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
        }

        private void FillTFVolume(String database, Int32 m, Int32 y)
        {
            string cmdText = @"insert into t_fVolume(nzp_kvar, nzp_serv, pu, norm, norm_full,  pu_odn, norm_odn, koef, cnt_stage, hvscnt2, hvscnt3,  gvscnt2, gvscnt3)  
SELECT a.nzp_kvar,a.nzp_serv,  sum(case when stek = 3 AND cnt_stage=1 then val2+val1+dlt_reval else 0 end ) as pu,  
sum(case when stek = 30 AND cnt1>0 then val1/cnt1 else 0 end) as norma,  sum(case when stek = 3 then val1+dlt_reval else 0 end) as norm_full,  
sum(case when cnt_stage =1 AND stek = 3 AND dop87<0 AND val1+val2+dlt_reval > 0  AND   abs(dop87)>val1+val2+dlt_reval then -(val1+val2+dlt_reval)   
when cnt_stage =1 AND stek = 3 AND  dop87<0 AND val1+val2+dlt_reval > 0 AND abs(dop87)<val1+val2+dlt_reval then dop87   when cnt_stage =1 AND stek = 3 AND 
dop87<0 AND val1+val2+dlt_reval<0  then  0   when cnt_stage =1 AND stek = 3 AND dop87>0 then dop87 else 0 end) as pu_odn,  
sum(case when cnt_stage <1 AND stek = 3 AND dop87<0 AND val1+val2+dlt_reval > 0  AND   abs(dop87)>val1+val2+dlt_reval then -(val1+val2+dlt_reval)   
when cnt_stage <1 AND stek = 3 AND  dop87<0 AND val1+val2+dlt_reval > 0 AND abs(dop87)<val1+val2+dlt_reval then dop87   when cnt_stage <1 AND stek = 3 
AND dop87<0 AND val1+val2+dlt_reval<0  then  0   when cnt_stage <1 AND stek = 3 AND dop87>0 then dop87 else 0 end) as norm_odn,  
max(case when stek = 3 then kf307 else 0 end), max(cnt_stage),  max(case when nzp_serv = 6 then cnt2 else 0 end),  
max(case when nzp_serv = 6 then cnt3 else 0 end),  max(case when nzp_serv = 9 then cnt2 else 0 end),  max(case when nzp_serv = 9 then cnt3 else 0 end)   
FROM bill01_charge_"+(y-2000).ToString("00")+".counters_"+m.ToString("00")+@" a, fsel_kvar b   WHERE a.nzp_kvar=b.nzp_kvar AND dat_charge is null   
AND stek in (3,30) AND nzp_type=3 AND kod_info<>24   GROUP BY 1,2";
            NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }

            cmdText = @"update t_fVolume SET norm = coalesce((SELECT max(value::numeric) FROM fbill_kernel.res_values 
WHERE nzp_res= t_fVolume.hvscnt3  AND nzp_x=1 AND nzp_y=t_fVolume.hvscnt2),0)  WHERE norm = 0 AND nzp_serv=6";
            cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }

            cmdText = @"update t_fVolume SET norm = coalesce((SELECT max(value::numeric) 
FROM fbill_kernel.res_values WHERE nzp_res= t_fVolume.gvscnt3  AND nzp_x=2 AND nzp_y=t_fVolume.gvscnt2),0)  WHERE norm = 0 AND nzp_serv=9";
            cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
        }

        private void UpdateFselKvar(String database)
        {
            string cmdText = @"update fsel_kvar SET uch = ( SELECT uch FROM bill01_data.kvar a  WHERE a.nzp_kvar=fsel_kvar.nzp_kvar);";
            NpgsqlCommand cmd1 = new NpgsqlCommand(cmdText, conn);
            try
            {
                cmd1.ExecuteNonQuery();
            }
            catch (Exception e)
            {

            }
        }

        private DataTable SelectPart1(String database)
        {
            //string connStr = "Server=192.168.1.25;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
            string cmdText = @"SELECT a.*,geu, rajon,t.town  FROM fsel_kvar a left outer join fbill_data.s_geu sg on a.nzp_geu=sg.nzp_geu  ,fbill_data.s_ulica u  ,
fbill_data.s_rajon r  left outer join fbill_data.s_town t on r.nzp_town=t.nzp_town WHERE a.nzp_ul = u.nzp_ul AND r.nzp_raj = u.nzp_raj  
AND nzp_kvar in (SELECT nzp_kvar FROM t_fkvar_prm WHERE is_open = 1 )  ORDER BY geu,rajon,ulica, idom, ndom, ikvar, nkvar";

            //cmdText = @"SELECT * FROM t_fkvar_prm";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);
            NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            try
            {
                da.Fill(dt);
                return dt;
            }
            catch(Exception e)
            {
                return null;
            }
        }

        private DataTable SelectFKvarPrm(String database, String nzp_kvar)
        {
            //string connStr = "Server=192.168.1.25;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
            string cmdText = @" SELECT *  FROM t_fkvar_prm  WHERE nzp_kvar = " + nzp_kvar;
            NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn);
            NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            try
            {
                da.Fill(dt);
                return dt;
            }
            catch(Exception ex)
            {
                return null;
            }
        }

        private DataTable SelectOrgInfo(String database)
        {
            //string connStr = "Server=192.168.1.25;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
            string cmdText = @"SELECT *   FROM fbill_data.s_bankstr  ORDER BY nzp_area, nzp_geu";
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

        private DataTable SelectRemark(String database, String nzp_dom, String nzp_geu, String nzp_area)
        {
            string connStr = "Server=192.168.1.25;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
            //string connStr = "Server=192.168.1.25;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
            string cmdText = @"SELECT 1 as ord, remark FROM fbill_data.s_remark WHERE nzp_dom = " + nzp_dom +
            " union all SELECT 2 , remark FROM fbill_data.s_remark WHERE nzp_geu=" + nzp_geu +
            " union all  SELECT 3, remark  FROM fbill_data.s_remark WHERE nzp_area=" + nzp_area;
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

        private DataTable SelectPrm2074(String database, String nzp_kvar)
        {
            //string connStr = "Server=192.168.1.25;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
            string cmdText = @"SELECT nzp, val_prm, dat_s, dat_po FROM bill01_data.prm_2   where nzp_prm = 2074 AND is_actual <> 100 and dat_po >= current_date 
                                AND nzp in (SELECT nzp_dom from bill01_data.kvar where nzp_kvar = "+nzp_kvar+")";
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
                case "radelit":
                    database = "RadElit";
                    break;
                case "kinel":
                    database = "kinel";
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

        /*public void LoadForGis(String db, Int32 year, Int32 month)
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
        }*/

        private DataTable SelectCounters(string database)
        {
            string connStr = "Server=192.168.1.25;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
            //string connStr = "Server=192.168.1.25;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
            string cmdText = @"SELECT ul.ulica || ', д.' || d.ndom || ' кв.'|| k.nkvar, k.fio, " + (database == "billAuk" ? "k.num_ls" : database == "RadElit" ? "k.pkod10" : "k.pkod") +
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
            catch(Exception e)
            {
                return null;
            }
        }

        private DataTable SelectPayments(string database, bool all, string dateFrom, string dateTo)
        {
            string connStr = "Server=192.168.1.25;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
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
                cmdText = @"SELECT " + (database == "RadElit" ? "k.pkod10" : "k.num_ls") + @", k.pkod, sum(pl.g_sum_ls) as g_sum_ls, to_char(pl.dat_vvod, 'dd-mm-yyyy') as dat_vvod,  b.bank as bank
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

                    cmdText += @" SELECT " + (database == "RadElit" ? "k.pkod10" : "k.num_ls") + @", k.pkod, sum(pl.g_sum_ls) as g_sum_ls, to_char(pl.dat_vvod, 'dd-mm-yyyy') as dat_vvod,  b.bank as bank
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
            //string connStr = "Server=192.168.1.25;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
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
            //string connStr = "Server=192.168.1.25;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
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
            //string connStr = "Server=192.168.1.25;Database=" + database + ";User ID=postgres;Password=Admin;CommandTimeout=180000;";
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

    public class BaseServ : ICloneable, IComparable<BaseServ>
    {
        //Услуга Коммунальная или нет
        public bool KommServ;

        /// <summary>
        /// Основаня сумма по услуге
        /// </summary>
        public SumServ Serv;
        /// <summary>
        /// в т.ч. сумма ОДН по услуге
        /// </summary>
        public SumServ ServOdn;

        public List<SumServ> SlaveServ;

        public BaseServ(bool hasOdn)
        {
            Serv = new SumServ();
            ServOdn = new SumServ();
            Serv.IsOdn = hasOdn;
            KommServ = false;
            SlaveServ = new List<SumServ>();
        }

        public void Clear()
        {
            Serv.Clear();
            ServOdn.Clear();
            SlaveServ.Clear();
        }

        /// <summary>
        /// Добавить к услуге некоторую сумму
        /// </summary>
        /// <param name="aServ"></param>
        public void AddSum(SumServ aServ)
        {
            Serv.AddSum(aServ);
            if (aServ.IsOdn)
            {
                ServOdn.AddSum(aServ);
            }
        }

        public void AddSlave(SumServ aServ)
        {
            bool findServ = false;
            foreach (SumServ ss in SlaveServ)
            {
                if (ss.NzpServ == aServ.NzpServ)
                {
                    ss.AddSum(aServ);
                    findServ = true;
                }
            }
            if (!findServ) SlaveServ.Add(aServ);
        }

        public bool Empty()
        {

            if ((Math.Abs(Serv.Tarif) < 0.001m) &
                (Math.Abs(Serv.RsumTarif) < 0.001m) &
                (Math.Abs(Serv.SumMoney) < 0.001m) &
                (Math.Abs(Serv.SumInsaldo) < 0.001m) &
                (Math.Abs(Serv.SumOutsaldo) < 0.001m) &
                (Math.Abs(Serv.RealCharge) < 0.001m) &
                (Math.Abs(Serv.Reval) < 0.001m)
                )
            {
                return true;
            }

            if ((Math.Abs(Serv.Tarif) > 0.001m) & (Serv.IsOdn) &
                (Math.Abs(Serv.RsumTarif) < 0.001m) &
                (Math.Abs(Serv.SumMoney) < 0.001m) &
                (Math.Abs(Serv.SumInsaldo) < 0.001m) &
                (Math.Abs(Serv.SumOutsaldo) < 0.001m) &
                (Math.Abs(Serv.RealCharge) < 0.001m) &
                (Math.Abs(Serv.Reval) < 0.001m))
            {
                return true;
            }
            return false;
        }

        public void CopyToOdn()
        {
            if (Serv.IsOdn)
            {
                ServOdn.AddSum(Serv);
                ServOdn.CCalc = Serv.CCalc;
                ServOdn.CReval = Serv.CReval;
            }
        }

        public object Clone()
        {
            var newServ = new BaseServ(false)
            {
                Serv = (SumServ)Serv.Clone(),
                ServOdn = (SumServ)ServOdn.Clone(),
                KommServ = KommServ,
                SlaveServ = new List<SumServ>()
            };
            foreach (SumServ ss in SlaveServ)
            {
                newServ.SlaveServ.Add((SumServ)ss.Clone());
            }

            return newServ;
        }

        public int CompareTo(BaseServ other)
        {

            if (other == null) return 1;

            return Serv.Ordering.CompareTo(other.Serv.Ordering);
        }
    }

    public class SumServ : ICloneable
    {
        public string NameServ;
        public string NameSupp;
        public string SuppRekv;
        public int NzpServ;
        public int NzpSupp;
        public int NzpFrm;
        public int IsDevice;
        public int NzpMeasure;
        public int OldMeasure;
        public int Ordering;
        public string Measure;
        public decimal SumInsaldo;
        public decimal SumOutsaldo;
        public decimal Tarif;
        public decimal TarifF;
        public decimal RsumTarif;
        public decimal SumTarif;
        public decimal SumLgota;
        public decimal SumNedop;
        public decimal SumReal;
        public decimal SumSn;
        public decimal Reval;
        public decimal RevalGil;
        public decimal SumPere;
        public decimal RealCharge;
        public decimal SumCharge;
        public decimal SumMoney;
        public decimal CCalc;
        public decimal Norma;
        public decimal CReval;
        public decimal Compensation;
        public bool UnionServ;
        public bool CanAddTarif;
        public bool CanAddVolume;
        public bool IsOdn;
        public int COkaz;
        public decimal OdnDomVolumePu; //текущие показания ПУ ком.услуг. на ОДН
        public SumServ()
        {
            Clear();
        }

        public void Clear()
        {
            NameServ = "";
            NameSupp = "";
            SuppRekv = "";
            Measure = "";
            NzpServ = 0;
            NzpSupp = 0;
            NzpFrm = 0;
            IsDevice = 0;
            NzpMeasure = 0;
            OldMeasure = 0;
            Ordering = 0;
            SumInsaldo = 0;
            SumOutsaldo = 0;
            Tarif = 0;
            TarifF = 0;
            RsumTarif = 0;
            SumTarif = 0;
            SumLgota = 0;
            SumNedop = 0;
            SumReal = 0;
            SumSn = 0;
            Reval = 0;
            RevalGil = 0;
            Norma = 0;
            SumPere = 0;
            RealCharge = 0;
            SumCharge = 0;
            SumMoney = 0;
            CCalc = 0;
            CReval = 0;
            UnionServ = false;
            CanAddTarif = true;
            CanAddVolume = true;
            IsOdn = false;
            COkaz = 0;
            Compensation = 0;
        }

        public void AddSum(SumServ aServ)
        {
            SumInsaldo += aServ.SumInsaldo;
            SumOutsaldo += aServ.SumOutsaldo;
            RsumTarif += aServ.RsumTarif;
            SumTarif += aServ.SumTarif;
            SumLgota += aServ.SumLgota;
            SumNedop += aServ.SumNedop;
            SumReal += aServ.SumReal;
            SumSn += aServ.SumSn;
            Reval += aServ.Reval;
            SumPere += aServ.SumPere;
            RealCharge += aServ.RealCharge;
            SumCharge += aServ.SumCharge;
            SumMoney += aServ.SumMoney;
            Norma += aServ.Norma;
            Compensation += aServ.Compensation;
            NzpSupp = aServ.NzpSupp;

            if (Tarif == 0) IsOdn = aServ.IsOdn;

            if (aServ.IsOdn == false)
                NzpFrm = aServ.NzpFrm;
            if ((NzpServ == 9) & (aServ.NzpServ == 14))
            {
                aServ.CanAddVolume = false;
            }
            if (NzpMeasure == 0)
                NzpMeasure = aServ.NzpMeasure;

            if (OldMeasure == 0)
                OldMeasure = aServ.OldMeasure;

            if ((aServ.Tarif > 0) & (aServ.IsOdn == false))
            {
                NzpMeasure = aServ.NzpMeasure;
                OldMeasure = aServ.OldMeasure;
            }



            if ((NzpMeasure == aServ.NzpMeasure) & (aServ.CanAddVolume))
            {
                CCalc += aServ.CCalc;
                CReval += aServ.CReval;
            }



            if (((CanAddTarif & aServ.CanAddTarif) || (NzpServ == aServ.NzpServ)) &
                (IsOdn == false))
            {
                Tarif += aServ.Tarif;
            }
            else
            {
                Tarif = Math.Max(Tarif, aServ.Tarif);
                // isOdn = aServ.isOdn;
            }

            if (NzpServ != aServ.NzpServ)
            {
                UnionServ = true;
            }
            if ((aServ.Tarif > 0) & (aServ.IsOdn == false)) IsDevice = Math.Max(IsDevice, aServ.IsDevice);

            COkaz = (COkaz == 0) ? aServ.COkaz : (aServ.COkaz < COkaz) ? aServ.COkaz : COkaz;

            if (aServ.IsOdn == false) IsOdn = false;

        }

        public object Clone()
        {
            var newServ = (SumServ)MemberwiseClone();
            return newServ;
        }

    }

    public class MasterServ
    {
        /// <summary>
        /// Услуга, к которой присоединяют остальные услуги
        /// </summary>
        public BaseServ MainServ;
        /// <summary>
        /// Список услуг, присоединяемых к основной услуге 
        /// </summary>
        public List<BaseServ> SlaveListServ;

        public MasterServ()
        {
            MainServ = new BaseServ(false);
            SlaveListServ = new List<BaseServ>();
        }

    }

    public class CUnionServ
    {
        public List<MasterServ> MasterList;

        public CUnionServ()
        {
            MasterList = new List<MasterServ>();
        }

        /// <summary>
        /// Получить услугу, в которую объединяют, по коду подчиненной услуги
        /// </summary>
        /// <param name="nzpServ">код подчиненной услуги</param>
        /// <returns>Базовая услуга</returns>
        public BaseServ GetMainServBySlave(int nzpServ)
        {
            return (from t in MasterList where t.SlaveListServ.Any(t1 => t1.Serv.NzpServ == nzpServ) select t.MainServ).FirstOrDefault();
        }

        /// <summary>
        /// Получить мастер услугу(со списком подчиненных услуг) по коду подчиненной услуги
        /// </summary>
        /// <param name="nzpServ">Код подчиненной услуги</param>
        /// <returns>Мастер услуга</returns>
        public MasterServ GetMasterBySlave(int nzpServ)
        {
            return MasterList.FirstOrDefault(t => t.SlaveListServ.Any(t1 => t1.Serv.NzpServ == nzpServ));
        }

        /// <summary>
        /// Добавить услугу в список объединенных услуг
        /// </summary>
        /// <param name="mainServ">Мастер услуга</param>
        /// <param name="slaveServ">Подчиненная услуга</param>
        public void AddServ(BaseServ mainServ, BaseServ slaveServ)
        {
            bool hasMaster = false;
            foreach (MasterServ t in MasterList)
            {
                if (t.MainServ.Serv.NzpServ == mainServ.Serv.NzpServ)
                {
                    bool hasSlave = false;
                    hasMaster = true;

                    foreach (BaseServ t1 in t.SlaveListServ)
                    {
                        if (t1.Serv.NzpServ == slaveServ.Serv.NzpServ)
                        {
                            hasSlave = true;
                        }
                    }
                    if (!hasSlave) //Если подчиненная услуга не была ранее добавлена, то добавляем её
                    {
                        t.SlaveListServ.Add(slaveServ);
                    }
                }
            }
            if (!hasMaster) //Если мастер услуга не найдена, то создает её и добавляет
            {
                var masterServ = new MasterServ { MainServ = mainServ };
                masterServ.SlaveListServ.Add(slaveServ);
                MasterList.Add(masterServ);
            }
        }
    }

    public class ServVolume
    {
        public int NzpServ;
        public string ServiceName;
        public decimal PUVolume;
        public decimal NormaVolume;
        public decimal NormaFullVolume;
        public decimal OdnFlatNormVolume;
        public decimal OdnFlatPuVolume;
        public decimal OdnDomVolume;
        public decimal DomVolume;
        public decimal DomLiftVolume;
        public decimal DomArendatorsVolume;
        public decimal Kf307;
        public decimal AllLsVolume;
        public int IsPu;
        public ServVolume()
        {
            Clear();
        }

        public void Clear()
        {
            NzpServ = 0;
            ServiceName = "";
            PUVolume = 0;
            NormaVolume = 0;
            NormaFullVolume = 0;
            OdnFlatNormVolume = 0;
            OdnFlatPuVolume = 0;
            OdnDomVolume = 0;
            DomVolume = 0;
            DomLiftVolume = 0;
            DomArendatorsVolume = 0;
            IsPu = 0;
            Kf307 = 0;
            AllLsVolume = 0;
        }

    }

    public struct Counters
    {
        public int NzpServ;//Код услуги
        public string ServiceName;//Наименование услуги
        public string NumCounters;//Заводской номер счетчика
        public string Place;//Место подключения счетчика в квартире
        public decimal Value;//Показание счетчика
        public DateTime DatUchet;//Дата показания счетчика
        public decimal ValuePred;//Предыдущее показание счетчика
        public DateTime DatUchetPred;//Дата предыдущего показания счетчика
        public int CntStage; //Разрядность счетчика
        public decimal Formula;//Масштабный множитель
        public string DatProv;//Дата поверки счетчика
        public bool IsGkal;//Признак Гигакаллорного счетчика
        public string Measure;//Единица измерения
    }
}
