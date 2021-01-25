﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;

namespace ETL
{
    class Program
    {
        #region Eigenschaften

        static readonly string DataSourceURL = "https://cbrell.de/bwi403/demo/getKlimaWS.php";
        static readonly string AnalyzeURL = "https://cbrell.de/bwi403/demo/analyseWS.php?x={0};{1}";
        static readonly string InDataFile = Path.GetTempPath() + "ein.csv";
        static readonly string OutDataFile = Path.GetTempPath() + "aus.csv";
        static readonly CultureInfo Culture = CultureInfo.CreateSpecificCulture("en-US");
        enum Werttyp
        {
            Temperatur,
            Luftfeuchte,
            Luftdruck,
            Feinstaub10,
            Feinstaub25
        }

        enum Zeittyp
        {
            Standard,
            Unix

        }
        #endregion

        static void Main(string[] args)
        {
            Zeittyp zeittyp = Zeittyp.Standard;
            Werttyp werttyp = Werttyp.Luftfeuchte;

            // ein.csv Laden
            List<KlimaWS> klimaWsListe = Load(DataSourceURL);

            // Analysieren
            Analyze(klimaWsListe, werttyp);

            // Extrahieren
            Extract(klimaWsListe, zeittyp, werttyp); // Extract(); kann auch so sein, standard Werttyp ist Temperatur         

        }

        #region Models
        class KlimaWS
        {
            public int Nr { get; set; }
            public int UnixTime { get; set; }
            public DateTime Zeit { get; set; }

            public decimal Temp { get; set; }
            public decimal Hum { get; set; }
            public decimal Druck { get; set; }
            public decimal PM10 { get; set; }
            public decimal PM25 { get; set; }

            /// <summary>
            /// 1 wenn zweite Zahl (nach Semikolon) größer als erste
            /// 0 wenn die Zahlen gleich
            /// -1 wenn zweite Zahl(nach Semikolon) kleiner ist als erste.
            /// </summary>
            public int Change { get; set; }

            public KlimaWS(string[] csvArray)
            {
                this.Nr = int.Parse(csvArray[0]);
                this.UnixTime = int.Parse(csvArray[1]);
                this.Zeit = Convert.ToDateTime(csvArray[2]);

                // https://docs.microsoft.com/en-us/dotnet/api/system.decimal.tryparse?view=net-5.0                
                decimal temp = decimal.Zero;
                this.Temp = decimal.TryParse(csvArray[3], NumberStyles.Number, Culture, out temp) ? temp : decimal.Zero;
                this.Hum = decimal.TryParse(csvArray[4], NumberStyles.Number, Culture, out temp) ? temp : decimal.Zero;
                this.Druck = decimal.TryParse(csvArray[5], NumberStyles.Number, Culture, out temp) ? temp : decimal.Zero;
                this.PM10 = decimal.TryParse(csvArray[6], NumberStyles.Number, Culture, out temp) ? temp : decimal.Zero;
                this.PM25 = decimal.TryParse(csvArray[7], NumberStyles.Number, Culture, out temp) ? temp : decimal.Zero;
            }

            public override string ToString()
            {
                return string.Format("{0};{1};{2};{3};{4};{5};{6};{7}"
                    , this.Nr.ToString()
                    , this.UnixTime.ToString()
                    , this.Zeit.ToString()
                    , this.Temp.ToString()
                     , this.Hum.ToString()
                     , this.Druck.ToString()
                     , this.PM10.ToString()
                    , this.PM25.ToString());
            }

            public string GetAsString(Zeittyp zeittyp = Zeittyp.Standard, Werttyp werttyp = Werttyp.Temperatur)
            {
                string strKlimaWS = this.Nr.ToString() + ";\t";

                switch (zeittyp)
                {
                    case Zeittyp.Standard:
                        strKlimaWS += this.Zeit.ToString("dd.MM.yy HH:mm") + ";\t";
                        break;
                    case Zeittyp.Unix:
                        strKlimaWS += this.UnixTime.ToString() + ";\t";
                        break;
                }

                switch (werttyp)
                {
                    case Werttyp.Temperatur:
                        strKlimaWS += this.Temp.ToString().Replace(',', '.');
                        break;
                    case Werttyp.Luftfeuchte:
                        strKlimaWS += this.Hum.ToString().Replace(',', '.');
                        break;
                    case Werttyp.Luftdruck:
                        strKlimaWS += this.Druck.ToString().Replace(',', '.');
                        break;
                    case Werttyp.Feinstaub10:
                        strKlimaWS += this.PM10.ToString().Replace(',', '.');
                        break;
                    case Werttyp.Feinstaub25:
                        strKlimaWS += this.PM25.ToString().Replace(',', '.');
                        break;
                }
                
                strKlimaWS += ";\t";
                strKlimaWS += this.Change.ToString();

                return strKlimaWS + "\r\n";
            }

        }

        #endregion

        #region ETL-Prozess Methoden

        /// <summary>
        /// Es steht nun der Inhalt der Datei aus dem Internet in der Variablen s zur
        /// Verfügung und kann weiter verarbeitet werden.
        /// 
        /// Bereinigt den Inhalt der eingegebenen CSV-Datei
        /// 
        /// Ref https://docs.microsoft.com/en-us/dotnet/api/system.net.webclient?view=net-5.0
        /// 
        /// </summary>
        /// <param name="sourceUrl">WebService URI</param>
        /// <returns> Datensätze anzahl</returns>
        static List<KlimaWS> Load(string sourceUrl)
        {
            if (string.IsNullOrWhiteSpace(sourceUrl))
            {
                Console.WriteLine("LoadInData() F##K");
                throw new ApplicationException("Specify the URI of the resource to retrieve.");
            }

            WebClient wClient = new WebClient();
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            Stream data = wClient.OpenRead(sourceUrl);
            StreamReader reader = new StreamReader(data);

            string content = reader.ReadToEnd();

            if (content.Length == 0)
                Console.WriteLine("LoadInData() F##K");
            else
                Console.WriteLine("CSV-Datei werden vom WebService {0} geladen", sourceUrl);

            // clean the input csv file content
            content = content.Replace("<pre>", string.Empty);
            content = content.Replace("</pre>", string.Empty);
            Console.WriteLine("CSV-Datei werden bereinigt");

            // write the Input
            File.WriteAllText(InDataFile, content);
            int recordCount = RecordsCount(InDataFile);
            Console.WriteLine("{0} Datensätze werden eingelesen", recordCount);

            List<KlimaWS> klimaWSListe = new List<KlimaWS>();
            foreach (string line in content.Split("\r\n").ToList().Skip(1))
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                var values = line.Split(';');
                KlimaWS klimaWS = new KlimaWS(values);

                klimaWSListe.Add(klimaWS);
            }

            data.Close();
            reader.Close();

            Console.WriteLine("LoadInData() Erledigt");

            return klimaWSListe;
        }

        private static void Analyze(List<KlimaWS> klimaWsListe, Werttyp werttyp = Werttyp.Temperatur)
        {
            WebClient wClient = new WebClient();
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            Console.WriteLine("{0} Analysieren ...", werttyp.ToString());
            for (int i = 0; i< klimaWsListe.Count; i++)
            {
                if (i == 0)
                    continue;

                string addrs = string.Empty;

                switch (werttyp)
                {
                    case Werttyp.Temperatur:
                        addrs = string.Format(Culture, AnalyzeURL, klimaWsListe[i].Temp, klimaWsListe[i - 1].Temp);
                        break;
                    case Werttyp.Luftfeuchte:
                        addrs = string.Format(Culture, AnalyzeURL, klimaWsListe[i].Hum, klimaWsListe[i - 1].Hum);
                        break;
                    case Werttyp.Luftdruck:
                        addrs = string.Format(Culture, AnalyzeURL, klimaWsListe[i].Druck, klimaWsListe[i - 1].Druck);
                        break;
                    case Werttyp.Feinstaub10:
                        addrs = string.Format(Culture, AnalyzeURL, klimaWsListe[i].PM10, klimaWsListe[i - 1].PM10);
                        break;
                    case Werttyp.Feinstaub25:
                        addrs = string.Format(Culture, AnalyzeURL, klimaWsListe[i].PM25, klimaWsListe[i - 1].PM25);
                        break;                    
                }

                Stream data = wClient.OpenRead(addrs);
                StreamReader reader = new StreamReader(data);
                klimaWsListe[i].Change = int.Parse(reader.ReadToEnd());                
            }

            Console.WriteLine("{0} Werten werden Analysiert.", klimaWsListe.Count);

        }
        static void Extract(List<KlimaWS> klimaWsListe, Zeittyp zeittyp = Zeittyp.Standard, Werttyp werttyp = Werttyp.Temperatur)
        {
            StringBuilder content = new StringBuilder();

            Console.WriteLine("{0} Extrahieren werden extrahiert", werttyp.ToString());

            content.AppendLine("Nr;\tZeit;\t\t\tWert;\tVeraenderung");
            foreach (KlimaWS kws in klimaWsListe)
                content.AppendLine(kws.GetAsString(zeittyp, werttyp));

            File.WriteAllText(OutDataFile, content.ToString());

            int recordCount = RecordsCount(OutDataFile);
            Console.WriteLine("{0} Datensätze werden extrahiert", recordCount);
        }


        #endregion

        #region Helper

        /// <summary>
        /// Datensätze Anzahl
        /// </summary>
        /// <param name="filePath">Datenquelle File</param>
        /// <returns>Datensätze Anzahl</returns>
        static int RecordsCount(string filePath)
        {
            string[] lsContent = File.ReadAllLines(filePath);
            return lsContent.Length;
        }

        #endregion

    }
}
