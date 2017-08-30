using Elasticsearch.Net;
using Nest;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

// C:\Projects\springy\springy\bin\Debug\springy --daysback 1

namespace springy
{
    public static class Program
    {
        static void Main(string[] args)
        {
            FileStream ostrm;
            StreamWriter writer;
            TextWriter oldOut = Console.Out;
            var cargs = new CmdLineParser();
            cargs.Parse(args);
            var useSSL = true;
            string urlPrefix = useSSL ? "https" : "http";
            string es_hostname = "thingprod";
            int daysBack = -1 * 24;
            bool showSummary = true;
            long docCount = 0;
            bool forRedshift = false;

            if (cargs.Arguments.ContainsKey("daysback"))
            {
                daysBack = -1 * (Convert.ToInt32(cargs.Arguments["daysback"][0]) * 24);
            }

            if (cargs.Arguments.ContainsKey("redshift"))
            {
                forRedshift = true;
            }

            DateTime a = DateTime.Today.Add(new TimeSpan(daysBack, 0, 0));
            DateTime b = DateTime.Today.Add(new TimeSpan((daysBack + 24), 0, 0)); ;
            long startEpochDate = ToUnixTime(a) + (3600 * 4);
            long endEpochDate = ToUnixTime(b) + (3600 * 4);

            if (cargs.Arguments.ContainsKey("summary"))
            {
                showSummary = true;
            }

            string username = Environment.UserName;
            Console.Write("Enter windows password:");
            string password = Utils.ReadPassword();

            var nodes = new Uri[]
            {
                new Uri(string.Format("{0}://{1}:{2}@{3}1.rti.reimbtech.com:9200/",
                    urlPrefix,
                    Uri.EscapeDataString(username.Trim()),
                    Uri.EscapeDataString(password.Trim()),
                    es_hostname)),
                new Uri(string.Format("{0}://{1}:{2}@{3}2.rti.reimbtech.com:9200/",
                    urlPrefix,
                    Uri.EscapeDataString(username.Trim()),
                    Uri.EscapeDataString(password.Trim()),
                    es_hostname)),
                new Uri(string.Format("{0}://{1}:{2}@{3}3.rti.reimbtech.com:9200/",
                    urlPrefix,
                    Uri.EscapeDataString(username.Trim()),
                    Uri.EscapeDataString(password.Trim()),
                    es_hostname)),
                new Uri(string.Format("{0}://{1}:{2}@{3}4.rti.reimbtech.com:9200/",
                    urlPrefix,
                    Uri.EscapeDataString(username.Trim()),
                    Uri.EscapeDataString(password.Trim()),
                    es_hostname)),
                new Uri(string.Format("{0}://{1}:{2}@{3}5.rti.reimbtech.com:9200/",
                    urlPrefix,
                    Uri.EscapeDataString(username.Trim()),
                    Uri.EscapeDataString(password.Trim()),
                    es_hostname)),
            };

            var pool = new StaticConnectionPool(nodes);
            var settings = new ConnectionSettings(pool);
            //settings.DisablePing();
            var client = new ElasticClient(settings);

            var nowEpoch = DateTimeOffset.Now.ToUnixTimeSeconds();
            var d = new Dictionary<long, int>();
            var h = new HashSet<string>();
           var r = client.Search<estypes.cs_exceptions>(s=>s.Index("cs-configuration").Type("cs-exceptions").Query(q=>q.MatchAll()).Sort(ss=>ss.Descending(p=>p.doccreated)).Scroll("5m"));
            ostrm = new FileStream(@"\\rtiufs2\MIS\Programming and Development\Programming and Development Shared\IDTs\CoderAlertES_ReportedExceptions\cs-excepions.txt", FileMode.OpenOrCreate, FileAccess.Write);
            writer = new StreamWriter(ostrm);


            //.Size(1)

            // Headings
            Console.SetOut(writer);

            var heading = string.Format("{0},{1},{2},{3},{4},{5},{6},{7}",
                        "Serial", "doccreated", "docno", "ifn", "pagenum",
                        "problemdesc", "termkey", "userid");
            Console.WriteLine(heading);
            
            while (r.Hits.Count > 0)
            {

                foreach (var hit in r.Hits)
                {
                    docCount++;
                    var docno = hit.Source.docno ?? "";
                    var doccreated = hit.Source.doccreated.ToString() ?? "";
                    var ifn = hit.Source.ifn ?? "";
                    var pagenum = hit.Source.pagenum.ToString() ?? "";
                    var problemdesc = hit.Source.problemdesc ?? "";
                    var termkey = hit.Source.termkey ?? "";
                    var userid = hit.Source.userid ?? "";
                    var dateTimeOffset = DateTimeOffset.FromUnixTimeSeconds(Convert.ToInt64(doccreated));
                    var t = dateTimeOffset.DateTime.Add(new TimeSpan(-4, 0, 0));
                    var output = string.Format("{0},{1},{2},{3},{4},{5},{6},{7}",
                                       docCount, t.ToString("MM/dd/yyyy HH:mm:ss"), docno, ifn, pagenum, problemdesc, termkey, userid);
                    Console.WriteLine(output);
                    
                }

                r = client.Scroll<estypes.cs_exceptions>("5m", r.ScrollId);
                
            }


            Console.SetOut(oldOut);
            writer.Close();
            ostrm.Close();

            Console.WriteLine("Thanks");
            string[] lines = System.IO.File.ReadAllLines(@"K:\IDTs\CoderAlertES_ReportedExceptions\cs-excepions.txt");

            foreach (string line in lines)
            {
                // Use a tab to indent each line of the file.
                Console.WriteLine("" + line);
            }
        }

        static long BuildHourlyKey(DateTime d)
        {
            long x = 0;
            TimeSpan t = d.TimeOfDay;

            x += t.Hours;
            x += d.Day * 100;
            x += d.Month * 10000;
            x += d.Year * 1000000;

            return x;
        }
        static DateTime FromUnixTime(this long unixTime)
        {
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            return epoch.AddSeconds(unixTime);
        }

        static long ToUnixTime(this DateTime date)
        {
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            return Convert.ToInt64((date - epoch).TotalSeconds);
        }
    }
}
