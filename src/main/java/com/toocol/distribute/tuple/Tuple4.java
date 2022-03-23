package com.toocol.distribute.tuple;

/**
 * @author ZhaoZhe (joezane.cn@gmail.com)
 * @date 2022/3/16 10:53
 */
@SuppressWarnings("all")
public class Tuple4<T1, T2, T3, T4> {
    private T1 first;
    private T2 second;
    private T3 third;
    private T4 fourth;

    public Tuple4(T1 first, T2 second, T3 third, T4 fourth) {
        this.first = first;
        this.second = second;
        this.third = third;
        this.fourth = fourth;
    }

    public T1 _1() {
        return this.first;
    }

    public T2 _2() {
        return this.second;
    }

    public T3 _3() {
        return this.third;
    }

    public T4 _4() {
        return this.fourth;
    }

    public T1 getFirst() {
        return first;
    }

    public void setFirst(T1 first) {
        this.first = first;
    }

    public T2 getSecond() {
        return second;
    }

    public void setSecond(T2 second) {
        this.second = second;
    }

    public T3 getThird() {
        return third;
    }

    public void setThird(T3 third) {
        this.third = third;
    }

    public T4 getFourth() {
        return fourth;
    }

    public void setFourth(T4 fourth) {
        this.fourth = fourth;
    }
}
