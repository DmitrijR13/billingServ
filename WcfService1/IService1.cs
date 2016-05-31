using System;
using System.Collections.Generic;
using System.ServiceModel;
using System.ServiceModel.Web;
using System.Xml;

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

        [OperationContract]
        void LoadForGis(String db, Int32 year, Int32 month);
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
