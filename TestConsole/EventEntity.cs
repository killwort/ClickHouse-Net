using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace ADBRO.EventHubClickHouseConsumer
{
    public class EventEntity : IEnumerable
    {
        public DateTime EventTime { get; set; } = DateTime.UtcNow;
        public int Type { get; set; } = 0;

        public string UserGuidString { get; set; } = "UserGuidString";

        public uint Publisher { get; set; } = 1;
        public uint Site { get; set; } = 2;
        public uint Slot { get; set; } = 3;

        public uint Advertiser { get; set; } = 11;
        public uint Campaign { get; set; } = 12;
        public uint Group { get; set; } = 13;
        public uint Advertisement { get; set; } = 14;

        public uint FraudValidationResult { get; set; } = 666;

        public ulong Image { get; set; } = 21;
        public string ImageURL { get; set; } = "ImageURL";
        public string PageURL { get; set; } = "PageURL";
        public string LinkURL { get; set; } = "LinkURL";

        public IEnumerable<uint> BrandSafetyCategories { get; set; } = new uint[] { 1, 23, 4, 5 };

        public IEnumerable<uint> PageContentCategories { get; set; } = new uint[] { 1, 0, 4, 5 };

        public EventEntity()
        {
            EventTime = DateTime.Now;
        }

        /// <summary>
        /// SQL query for bulk insert.
        /// </summary>
        /// TODO: Remove ImageURL in scope of ADBRO-562
        public const string BULK_INSERT_SQL = "INSERT INTO events (EventTime, Type, UserGuidString, Publisher, Site, Slot," +
            "Advertiser, Campaign, Group, Advertisement, FraudValidationResult, Image, ImageURL, PageURL, LinkURL, BrandSafetyCategories, PageContentCategories) VALUES @bulk";

        public IEnumerator GetEnumerator()
        {
            // Count and order of returns must match column order in BULK_INSERT_SQL.
            yield return EventTime;
            yield return Type;

            yield return UserGuidString;

            yield return Publisher;
            yield return Site;
            yield return Slot;

            yield return Advertiser;
            yield return Campaign;
            yield return Group;
            yield return Advertisement;

            yield return FraudValidationResult;

            yield return Image;
            yield return ImageURL; // TODO: Remove in scope of ADBRO-562
            yield return PageURL;
            yield return LinkURL;

            yield return BrandSafetyCategories;
            yield return PageContentCategories;
        }

        //private IEnumerable<object> UintEnumAsObjectEnum(IEnumerable<uint> col)
        //{
        //    if (col != null)
        //    {
        //       foreach (var c in col)
        //        {
        //            yield return (uint)c;
        //        }
        //    }
        //}
    }
}
