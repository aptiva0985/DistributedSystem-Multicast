package distSysLab2.model;

import java.util.ArrayList;

public class GroupBean {
    private String name;
    private ArrayList<String> memberList;

    public GroupBean() {
        this.memberList = new ArrayList<String>();
    }
    
    public GroupBean(String name) {
        this.name = name;
        this.memberList = new ArrayList<String>();
    }

    public GroupBean(String name, ArrayList<String> memberList) {
        this.name = name;
        this.memberList = memberList;
    }

    public void addMember(String newUser) {
        this.memberList.add(newUser);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ArrayList<String> getMemberList() {
        return memberList;
    }

    public void setMemberList(ArrayList<String> memberList) {
        this.memberList = memberList;
    }

    @Override
    public String toString() {
        return "Group [groupName=" + name + ", memberList=" + memberList + "]";
    }
}
