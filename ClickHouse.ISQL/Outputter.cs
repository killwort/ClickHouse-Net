namespace ClickHouse.Isql {
    internal abstract class Outputter {
        public abstract void ResultStart();
        public abstract void ResultEnd();
        public abstract void RowStart();
        public abstract void RowEnd();

        public abstract void HeaderCell(string name);
        public abstract void ValueCell(object value);

        public abstract void DataStart();

        public virtual void Start() { }

        public virtual void End() { }
    }
}