import sys
import pandas as pd 
from scipy import stats
from statsmodels.stats import multicomp
import statsmodels.api as sm
import matplotlib.pyplot as plt

def main():

    model1 = pd.read_csv("originalmodelsim.csv")
    model2 = pd.read_csv("newmodelsim(gelu).csv")
    model3 = pd.read_csv("newmodelsim(weighted_data).csv")
    model4 = pd.read_csv("newmodelsim(more_conv_layers).csv")
    model5 = pd.read_csv("newmodelsim(dif_act_funct).csv")
    
    
    # Accuracy Tukey
    accuracy_df = pd.DataFrame({
        "Original": model1["accuracy%"],
        "Gelu": model2["accuracy%"],
        "Wighted Data": model3["accuracy%"],
        "Extra Layer": model4["accuracy%"],
        "Swish": model5["accuracy%"]
    })
    accuracy_melt = accuracy_df.melt()
    tukey_accuracy = multicomp.pairwise_tukeyhsd(
        accuracy_melt['value'], accuracy_melt['variable'], alpha=0.05
    )
    print(tukey_accuracy)

    fig = tukey_accuracy.plot_simultaneous()
    ax = fig.axes[0]
    ax.set_title("Accuracy Percentage Confidence Intervals")
    plt.show()
    
    
    # AvgSPLoss Tukey
    AvgSPLoss_df = pd.DataFrame({
        "Original": model1["AvgSPLoss"],
        "Gelu": model2["AvgSPLoss"],
        "Wighted Data": model3["AvgSPLoss"],
        "Extra Layer": model4["AvgSPLoss"],
        "Swish": model5["AvgSPLoss"]

    })
    AvgSPLoss_melt = AvgSPLoss_df.melt()
    tukey_sploss = multicomp.pairwise_tukeyhsd(
        AvgSPLoss_melt['value'], AvgSPLoss_melt['variable'], alpha=0.05
    )
    print(tukey_sploss)

    fig = tukey_sploss.plot_simultaneous()
    ax = fig.axes[0]
    ax.set_title("Average Centipawn Loss Confidence Intervals")
    plt.show()
    
    
    # Blunder Tukey
    Blunder_df = pd.DataFrame({
        "Original": model1["Blunder"],
        "Gelu": model2["Blunder"],
        "Wighted Data": model3["Blunder"],
        "Extra Layer": model4["Blunder"],
        "Swish": model5["Blunder"]

    })
    Blunder_melt = Blunder_df.melt()
    tukey_Blunder = multicomp.pairwise_tukeyhsd(
        Blunder_melt['value'], Blunder_melt['variable'], alpha=0.05
    )
    print(tukey_Blunder)

    fig = tukey_Blunder.plot_simultaneous()
    ax = fig.axes[0]
    ax.set_title("Blunders Confidence Intervals")
    plt.show()
    

if __name__ == '__main__':
    main()


