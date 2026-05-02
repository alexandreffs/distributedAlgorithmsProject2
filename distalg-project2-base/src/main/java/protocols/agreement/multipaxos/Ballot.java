package protocols.agreement.multipaxos;

import java.util.Objects;

public class Ballot implements Comparable<Ballot> {

    private final int number;
    private final String leaderId;

    public Ballot(int number, String leaderId) {
        this.number = number;
        this.leaderId = leaderId;
    }

    public int getNumber() {
        return number;
    }

    public String getLeaderId() {
        return leaderId;
    }

    @Override
    public int compareTo(Ballot other) {
        int cmp = Integer.compare(this.number, other.number);

        if (cmp != 0) {
            return cmp;
        }

        return this.leaderId.compareTo(other.leaderId);
    }

    public boolean greaterThan(Ballot other) {
        return compareTo(other) > 0;
    }

    public boolean greaterOrEqualThan(Ballot other) {
        return compareTo(other) >= 0;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Ballot)) {
            return false;
        }

        Ballot ballot = (Ballot) o;
        return number == ballot.number && Objects.equals(leaderId, ballot.leaderId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(number, leaderId);
    }

    @Override
    public String toString() {
        return "Ballot{" +
                "number=" + number +
                ", leaderId='" + leaderId + '\'' +
                '}';
    }
}