package com.king.demo.test;

/**
 * @author DKing
 * @description
 * @date 2019/5/26
 */
public class ThreadTest {

    public static void main(String[] args) {
        meiXiNiTanShangShiErLe();
    }

    public static void meiXiNiTanShangShiErLe() {
        Thread a = new Thread(() -> {
            System.out.println("梅西呵呵哒");

            try {
                Thread.sleep(2000 * 60 * 60);
            } catch (InterruptedException e) {

            }

            System.out.println("滚去锻炼");
        });

        Thread b = new Thread(() -> {
            for (int i = 5; i > -1; i--) {
                System.out.println("别玩了！" + i + "秒");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            a.interrupt();
        });

        a.start();
        b.start();
    }
}
