package org.apache.spark.ml.fed.event;

public class AlchemyJobEvent {

    private String jobID;
    /**
     * guest / arbiter
     */
    private String roleType;

    /**
     * arbiter dir path
     */
    private String arbiterContextPath;

    /**
     * guest file path
     */
    private String guestContextPath;

    public String getJobID() {
        return jobID;
    }

    public void setJobID(String jobID) {
        this.jobID = jobID;
    }

    public String getRoleType() {
        return roleType;
    }

    public void setRoleType(String roleType) {
        this.roleType = roleType;
    }

    public String getArbiterContextPath() {
        return arbiterContextPath;
    }

    public void setArbiterContextPath(String arbiterContextPath) {
        this.arbiterContextPath = arbiterContextPath;
    }

    public String getGuestContextPath() {
        return guestContextPath;
    }

    public void setGuestContextPath(String guestContextPath) {
        this.guestContextPath = guestContextPath;
    }

    @Override
    public String toString() {
        return "AlchemyJobEvent{" +
                "jobID='" + jobID + '\'' +
                ", roleType='" + roleType + '\'' +
                ", arbiterContextPath='" + arbiterContextPath + '\'' +
                ", guestContextPath='" + guestContextPath + '\'' +
                '}';
    }
}
