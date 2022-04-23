public class Customer {
    @Override
    public String toString() {
        return "Customer{" +
                "customerID=" + customerID +
                ", customerName='" + customerName + '\'' +
                '}';
    }

    private int customerID;
    private String customerName;
    public Customer(int ID, String name) {
        this.customerID = ID;
        this.customerName = name;
    }
    public int getID() {
        return customerID;
    }
    public String getName() {
        return customerName;
    }
}