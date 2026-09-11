package main

import "testing"

func TestBollinger(t *testing.T) {
	up, down := bollinger([]float64{1, 2, 3, 4, 5}, 2)
	if up < 5.82 || up > 5.83 || down < 0.17 || down > 0.18 {
		t.Fatalf("unexpected bands: up=%v down=%v", up, down)
	}
}

func TestSafePricesAvoidTaker(t *testing.T) {
	up, down, err := safePrices(99, 101, 100)
	if err != nil || up != 110 || down != 90 {
		t.Fatalf("unexpected prices: up=%v down=%v err=%v", up, down, err)
	}
}

func TestSafePricesRejectsNonPositiveBuy(t *testing.T) {
	if _, _, err := safePrices(101, 1, 1); err == nil {
		t.Fatal("expected non-positive buy error")
	}
}
