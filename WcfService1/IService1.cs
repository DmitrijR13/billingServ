using System;
using System.Collections.Generic;
using System.ServiceModel;
using System.ServiceModel.Web;
using System.Xml;
using XmlClass;

namespace ServiceFromBill
{
    // ПРИМЕЧАНИЕ. Команду "Переименовать" в меню "Рефакторинг" можно использовать для одновременного изменения имени интерфейса "IService1" в коде и файле конфигурации.
    [ServiceContract]
    public interface IService1
    {
        [WebGet(ResponseFormat = WebMessageFormat.Xml)]
        [OperationContract]
        List<ЛС> GetCounters(string db);

        [WebGet(ResponseFormat = WebMessageFormat.Xml)]
        [OperationContract]
        List<Payment> GetPayments(string db, bool all = false, string dateFrom = "", string dateTo = "");

        [WebGet(ResponseFormat = WebMessageFormat.Xml)]
        [OperationContract]
        List<ЛицевойСчет> GetFactura(String db, Int32 m, Int32 y, Int32 part, Int32 numLs = 0);

        //[OperationContract]
        //void LoadForGis(String db, Int32 year, Int32 month);
    }

    public class ЛС
    {
        public String Адресс { get; set; }
        public String ФИО { get; set; }
        public String Номер_ЛС { get; set; }
        public List<Услуга> Услуги { get; set; }
    }

    public class Услуга
    {
        public String Наименование { get; set; }
        public List<Счетчик> Счетчики { get; set; }
    }

    public class Счетчик
    {
        public String ID { get; set; }
        public String Дата_закрытия { get; set; }
        public String Номер { get; set; }
        public List<Показание> Показания { get; set; }
    }

    public class Показание
    {
        public String Год { get; set; }
        public String Месяц { get; set; }
        public String Значение { get; set; }
    }

    public class Payment
    {
        public String NumLs { get; set; }
        public String Pkod { get; set; }
        public String PaymentSum { get; set; }
        public String PaymentDate { get; set; }
        public String Bank { get; set; }
    }
}

namespace XmlClass
{
    public class ЛицевойСчет
    {
        public Раздел1 Раздел1 { get; set; }

        public Раздел2 Раздел2 { get; set; }

        public Раздел3 Раздел3 { get; set; }

        public Раздел4 Раздел4 { get; set; }

        public String Примечание { get; set; }
    }

    public class Раздел1
    {
        public String Период { get; set; }
        public String Приватизирована { get; set; }
        public String НомерЛС { get; set; }
        public String ФИО { get; set; }
        public String АдресПомещения { get; set; }
        public String ПлощадьПомещения { get; set; }
        public String ПрописаноПроживает { get; set; }
        public String ПлощадьДома { get; set; }
        public String ПлощадьМОП { get; set; }
        public String Проживает { get; set; }
        public String Организация { get; set; }
    }

    public class Раздел2
    {
        public String ПолучательПлатежа { get; set; }
        public String БанковскийСчет { get; set; }
        public String ПлатежныйКод { get; set; }
        public String ВидПлаты { get; set; }
        public String СуммаКОплате { get; set; }
    }

    public class Раздел3
    {
        public List<Услуга> Услуги { get; set; }

        public Итого Итого { get; set; }
    }

    public class Раздел4
    {
        public List<СправочнаяИнформация> СправочныеДанные { get; set; }
    }

    public class Услуга
    {
        public String ВидУслуги { get; set; }
        public String ЕдиницаИзмерения { get; set; }
        public ОбъемКоммунальныхУслуг ОбъемКоммунальныхУслуг { get; set; }
        public String Тариф { get; set; }
        public РазмерПлатыЗаКоммунальныеУслуги РазмерПлатыЗаКоммунальныеУслуги { get; set; }
        public String ВсегоНачислено { get; set; }
        public String Перерасчеты { get; set; }
        public String Льготы { get; set; }
        public ИтогоКОплате ИтогоКОплате { get; set; }
    }

    public class ОбъемКоммунальныхУслуг
    {
        public String ИндивидуальноеПотребление { get; set; }
        public String ОбщедомовыеНужды { get; set; }

        public ОбъемКоммунальныхУслуг()
        {
            ИндивидуальноеПотребление = "";
            ОбщедомовыеНужды = "";
        }
    }

    public class РазмерПлатыЗаКоммунальныеУслуги
    {
        public String ИндивидуальноеПотребление { get; set; }
        public String ОбщедомовыеНужды { get; set; }

        public РазмерПлатыЗаКоммунальныеУслуги()
        {
            ИндивидуальноеПотребление = "";
            ОбщедомовыеНужды = "";
        }
    }

    public class ИтогоКОплате
    {
        public String Всего { get; set; }
        public ЗаКоммульныеУслуги ЗаКоммульныеУслуги { get; set; }

        public ИтогоКОплате()
        {
            Всего = "";
            ЗаКоммульныеУслуги = new ЗаКоммульныеУслуги();
        }
    }

    public class ЗаКоммульныеУслуги
    {
        public String ИндивидуальноеПотребление { get; set; }
        public String ОбщедомовыеНужды { get; set; }

        public ЗаКоммульныеУслуги()
        {
            ИндивидуальноеПотребление = "";
            ОбщедомовыеНужды = "";
        }
    }

    public class Итого
    {
        public String РазмерПлатыИндивид { get; set; }
        public String РазмерПлатыДом { get; set; }
        public String ВсегоНачислено { get; set; }
        public String Перерасчеты { get; set; }
        public String Всего { get; set; }
        public String ИтогоИндивид { get; set; }
        public String ИтогоДом { get; set; }
        public String Долг { get; set; }
        public String Оплачено { get; set; }

        public Итого()
        {
            РазмерПлатыИндивид = "0";
            РазмерПлатыДом = "0";
            ВсегоНачислено = "0";
            Перерасчеты = "0";
            Всего = "0";
            ИтогоИндивид = "0";
            ИтогоДом = "0";
            Долг = "0";
            Оплачено = "0";
        }
    }

    public class СправочнаяИнформация
    {
        public String ВидУслуги { get; set; }
        public НормативПотребления НормативПотребления { get; set; }
        public Показания Показания { get; set; }
        public ОбъемКоммунальныхУслуг4 ОбъемКоммунальныхУслуг4 { get; set; }

        public СправочнаяИнформация()
        {
            НормативПотребления = new НормативПотребления();
            Показания = new Показания();
            ОбъемКоммунальныхУслуг4 = new ОбъемКоммунальныхУслуг4();
        }
    }

    public class НормативПотребления
    {
        public String Индивидуальное { get; set; }
        public String Общедомовое { get; set; }

        public НормативПотребления()
        {
            Индивидуальное = "";
            Общедомовое = "";
        }
    }

    public class Показания
    {
        public String Индивидуальные { get; set; }
        public String Общедомовые { get; set; }

        public Показания()
        {
            Индивидуальные = "";
            Общедомовые = "";
        }
    }

    public class ОбъемКоммунальныхУслуг4
    {
        public String ПомещенияДома { get; set; }
        public String ОбщедомовыеНуждыДома { get; set; }

        public ОбъемКоммунальныхУслуг4()
        {
            ПомещенияДома = "";
            ОбщедомовыеНуждыДома = "";
        }
    }
}

