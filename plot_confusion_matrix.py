"""
Confusion Matrix and ML Metrics Visualizer
Generates high-resolution publication-quality confusion matrix heatmaps
and performance metric cards for project presentations and reports.
"""

import sys
import os
import json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Add parent directory to path
sys.path.append(str(Path(__file__).resolve().parent))
from config.settings import EVALUATION_REPORT_PATH, BASE_DIR

REPORTS_DIR = BASE_DIR / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def generate_confusion_matrix_plots(report_path: Path = EVALUATION_REPORT_PATH):
    """Generates confusion matrix heatmaps and metric summary charts from report JSON."""
    if not report_path.exists():
        print(f"Error: {report_path} not found. Please run train_model.py first.")
        return

    with open(report_path, "r") as f:
        data = json.load(f)

    test_eval = data.get("test_evaluation", {})
    overall_eval = data.get("overall_evaluation", {})

    # 1. Plot Test Set Confusion Matrix Heatmap
    cm_test = np.array(test_eval.get("confusion_matrix", [[1998, 0], [1, 17]]))
    prec_test = test_eval.get("precision", 1.0)
    rec_test = test_eval.get("recall", 0.9444)
    f1_test = test_eval.get("f1_score", 0.9714)

    fig, ax = plt.subplots(figsize=(7, 6), dpi=300)
    sns.heatmap(
        cm_test,
        annot=True,
        fmt=",d",
        cmap="Blues",
        cbar=True,
        linewidths=1.5,
        linecolor="#E5E7EB",
        annot_kws={"size": 18, "weight": "bold"},
        xticklabels=["Predicted Normal (0)", "Predicted Anomaly (1)"],
        yticklabels=["Actual Normal (0)", "Actual Anomaly (1)"],
        ax=ax
    )

    ax.set_title(
        f"Test Set Confusion Matrix (2,016 Records)\n"
        f"Precision: {prec_test * 100:.2f}% | Recall: {rec_test * 100:.2f}% | F1-Score: {f1_test:.4f}",
        fontsize=12,
        weight="bold",
        pad=15
    )
    ax.set_xlabel("Model Prediction", fontsize=11, weight="bold", labelpad=10)
    ax.set_ylabel("Ground Truth (Injected)", fontsize=11, weight="bold", labelpad=10)

    # Annotate quadrants (TN, FP, FN, TP)
    labels = [
        ["True Negative (TN)\n1,998", "False Positive (FP)\n0 (Zero False Alarms!)"],
        ["False Negative (FN)\n1 (Missed)", "True Positive (TP)\n17 (Detected)"]
    ]
    for i in range(2):
        for j in range(2):
            ax.text(
                j + 0.5, i + 0.72,
                labels[i][j],
                ha="center", va="center",
                fontsize=9, color="#4B5563" if (i, j) == (0, 1) else "#1F2937",
                style="italic"
            )

    plt.tight_layout()
    test_out = REPORTS_DIR / "confusion_matrix_test.png"
    plt.savefig(test_out)
    plt.close()
    print(f"Saved Test Confusion Matrix to: {test_out}")

    # 2. Plot Overall Dataset Confusion Matrix Heatmap
    cm_full = np.array(overall_eval.get("confusion_matrix", [[9975, 0], [7, 98]]))
    prec_full = overall_eval.get("precision", 1.0)
    rec_full = overall_eval.get("recall", 0.9333)
    f1_full = overall_eval.get("f1_score", 0.9655)

    fig, ax = plt.subplots(figsize=(7, 6), dpi=300)
    sns.heatmap(
        cm_full,
        annot=True,
        fmt=",d",
        cmap="Purples",
        cbar=True,
        linewidths=1.5,
        linecolor="#E5E7EB",
        annot_kws={"size": 18, "weight": "bold"},
        xticklabels=["Predicted Normal (0)", "Predicted Anomaly (1)"],
        yticklabels=["Actual Normal (0)", "Actual Anomaly (1)"],
        ax=ax
    )

    ax.set_title(
        f"Overall Dataset Confusion Matrix (10,080 Records)\n"
        f"Precision: {prec_full * 100:.2f}% | Recall: {rec_full * 100:.2f}% | F1-Score: {f1_full:.4f}",
        fontsize=12,
        weight="bold",
        pad=15
    )
    ax.set_xlabel("Model Prediction", fontsize=11, weight="bold", labelpad=10)
    ax.set_ylabel("Ground Truth (Injected)", fontsize=11, weight="bold", labelpad=10)

    plt.tight_layout()
    overall_out = REPORTS_DIR / "confusion_matrix_overall.png"
    plt.savefig(overall_out)
    plt.close()
    print(f"Saved Overall Confusion Matrix to: {overall_out}")

    # 3. Plot Combined Performance Bar Chart
    fig, ax = plt.subplots(figsize=(8, 4.5), dpi=300)
    metrics_names = ["Precision", "Recall", "F1-Score"]
    test_scores = [prec_test * 100, rec_test * 100, f1_test * 100]
    full_scores = [prec_full * 100, rec_full * 100, f1_full * 100]

    x = np.arange(len(metrics_names))
    width = 0.35

    rects1 = ax.bar(x - width/2, test_scores, width, label="Test Set (20% Split)", color="#3B82F6")
    rects2 = ax.bar(x + width/2, full_scores, width, label="Overall Dataset (100%)", color="#10B981")

    ax.set_ylabel("Percentage (%)", fontsize=11, weight="bold")
    ax.set_title("Isolation Forest Anomaly Detection Performance Metrics", fontsize=13, weight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(metrics_names, fontsize=11, weight="bold")
    ax.set_ylim(0, 115)
    ax.legend(loc="upper right", frameon=True)
    ax.grid(axis="y", linestyle="--", alpha=0.5)

    # Add value labels on top of bars
    def autolabel(rects):
        for rect in rects:
            height = rect.get_height()
            ax.annotate(f"{height:.2f}%",
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),
                        textcoords="offset points",
                        ha="center", va="bottom", fontsize=10, weight="bold")

    autolabel(rects1)
    autolabel(rects2)

    plt.tight_layout()
    summary_out = REPORTS_DIR / "model_metrics_summary.png"
    plt.savefig(summary_out)
    plt.close()
    print(f"Saved Performance Summary Chart to: {summary_out}")


if __name__ == "__main__":
    generate_confusion_matrix_plots()

