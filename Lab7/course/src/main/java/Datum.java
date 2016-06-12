/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author I3OR2A
 */
public class Datum {

    private String id;
    private int num;
    private double val;

    public Datum(String id, int num, double val) {
        this.id = id;
        this.num = num;
        this.val = val;
    }

    public String getId() {
        return id;
    }

    public int getNum() {
        return num;
    }

    public double getVal() {
        return val;
    }
}
