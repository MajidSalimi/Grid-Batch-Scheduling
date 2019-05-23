
//Written by: Majid Salimi
//MinMin, MaxMin, Suffrage and Genetic for batch task scheduling in a distributed environment (grid environment)
//before running the algorithm, you should set the directory of input data (braun dataset) in the main(), number of resources (nres), and number of tasks (ntask) based on the selected input data. (default values: inputdata=u_c_hihi.0    ;    nres = 16    ;    ntask = 512 )
           
        

import java.util.*;
import java.io.*;
import java.text.DecimalFormat;
import jdk.nashorn.internal.runtime.FindProperty;

public class Scheduling {

    private double[][] MinOfCols;
    int nres, ntask;//No. of Resources and No. of Tasks
    String fname;// input file name
    private double[][] ETC, TMPETC;
    //private int [][] alloc;// final allocation matrix
    // private int [] MinResNo;// Resoutce No with min ETC
    private int[] TaskNo;//   Task No with min ETC a
    private int[] JobsNumber;
    double[][] DifferenceOfMins;
    double[] NumOfUsageOfRes;
    double MinOfMins, MaxOfMins;
    double[] ResUtilization;
    double[][] Solution;
    double[] Chromosome1;
    double[] Chromosome2;
    int SelectedJob = 0;
//	PrintStream stdout = System.out;

    public static void prt(String s) {
        System.out.print(s);
    }

    /**
     * read etc from input file
     *
     */
    private void read_etc(String fn) {
        int i, j;
        fname = fn;
        try {
            Scanner inf = new Scanner(new File(fname));
            // read no. of resources and tasks
            //if we want to run with brown data,erase "//"
            //nres = inf.nextInt(); // No. of Resources
            //ntask = inf.nextInt(); // No. of Tasks
            nres = 16;
            ntask = 512;

            // Allocate space for ETC, TMPETC and alloc matrices
            alloc_space(nres, ntask);

            // read ETC matrix from file
            for (j = 0; j < ntask; j++){
            for (i = 0; i < nres; i++) {
                    ETC[i][j] = inf.nextDouble();
                    TMPETC[i][j] = ETC[i][j];
                }
            }

            // Store Task No. in TaskNo array	
            for (j = 0; j < ntask; j++) {
                TaskNo[j] = j;
            }

            inf.close();// close input file
        } catch (Exception e) {
            prt("\n\nError in Save!" + e);
        }
        print_etc();// print ETC matrix
    }// end read_resource_task

    /**
     * Allocate space for ETC, TMPETC and alloc matrices
     *
     */
    private void alloc_space(int res, int task) {
        ETC = new double[res][task];
        TMPETC = new double[res + 1][task + 1];
        // alloc    = new int[res][task+1];
        TaskNo = new int[res];
        // MinResNo = new int[res];
        JobsNumber = new int[ntask + 1];
    }// end alloc_space

    /**
     * print ETC
     *
     */
    private void print_etc() {
        prt("\nETC Matrix is as follows: No. of Resources = " + nres
                + ", No. of Tasks = " + ntask + "\n");

        for (int i = 0; i < ntask; i++) {
            prt("\tT" + i);
        }

        for (int i = 0; i < nres; i++) {
            prt("\n RES " + i + ":");
            for (int j = 0; j < ntask; j++) {
                prt("\t" + ETC[i][j]);
            }
        }
        prt("\n");
    }// end print_etc

    /**
     * min min scheduling
     *
     */
    private void minmin() {
        DecimalFormat decimal = new DecimalFormat("#.######");
        double Utilization, MakeSpan;
        NumOfUsageOfRes = new double[nres];
        for (int i = 1; i <= ntask; i++) {
            JobsNumber[i] = i;
        }
        SelectedJob = 0;
        // code for min min scheduling
        for (int k = ntask; k > 0; k--) {
            {
                //PrintTMPETCArray();
                FindResMin(k);//find every column's minimum
                SelectedJob = FindBestRes(k);//returns index of Minimum of minimum jobs
                MinOfMins=ETC[(int) MinOfCols[1][SelectedJob]][JobsNumber[SelectedJob]];
                System.out.println("Res:" + (int) MinOfCols[1][SelectedJob] + "-->job:" + JobsNumber[SelectedJob] + "\n");
                NumOfUsageOfRes[(int) MinOfCols[1][SelectedJob]]++;//hold sum of usage of resources to calculate makespan
                AddJobTimeToRes(MinOfMins, k, (int) MinOfCols[1][SelectedJob]);//add selected job runtime to the row of selected resource
                ReplaceColumns(k, SelectedJob);//rearrange columns and put 0 to the runned job column

               // PrintTMPETCArray();
            }// inner for    

        } // outer for
        System.out.println("-----------------------------------------------------------------------------\n");
        String MKSPN = String.valueOf(CalcMakeSpan());
        MakeSpan = CalcMakeSpan();
        System.out.println("Makespan: " + MKSPN);
        Utilization = CalcUtilization(MakeSpan);
        System.out.println("Utilization: " + Double.valueOf(decimal.format(Utilization * 100)) + " %\n\n");
    }// end minmin

    /**
     * max min scheduling
     *
     */
    private void maxmin() {
        double MakeSpan, Utilization;
        DecimalFormat decimal = new DecimalFormat("#.######");
        NumOfUsageOfRes = new double[nres];
        for (int i = 1; i <= ntask; i++) {
            JobsNumber[i] = i;
        }
        SelectedJob = 0;
        double ColMin, RowMin;
        // code for min min scheduling
        for (int k = ntask; k > 0; k--) {
            {
                //PrintTMPETCArray();
                FindResMin(k);//find every column's minimum
                SelectedJob = FindBestResMax(k);//find maximum of minimums
                MaxOfMins=ETC[(int) MinOfCols[1][SelectedJob]][JobsNumber[SelectedJob]];
                System.out.println("Res:" + (int) MinOfCols[1][SelectedJob] + "-->job:" + JobsNumber[SelectedJob] + "\n");
                AddJobTimeToRes(MaxOfMins, k, (int) MinOfCols[1][SelectedJob]);//add selected job runtime to the row of selected resource
                ReplaceColumns(k, SelectedJob);//rearrange columns and put 0 to the runned job column
                
                //PrintTMPETCArray();
            }// inner for    

        } // outer for   
        System.out.println("-----------------------------------------------------------------------------\n");
        MakeSpan = CalcMakeSpan();
        System.out.println("Makespan: " + MakeSpan);
        Utilization = CalcUtilization(MakeSpan);
        System.out.println("Utilization: " + Double.valueOf(decimal.format(Utilization * 100)) + " %\n\n");
    }

    /**
     * suffrage scheduling
     *
     */
    private void suffrage() {
        double MakeSpan = 0, Utilization = 0;
        DecimalFormat decimal = new DecimalFormat("#.######");
        NumOfUsageOfRes = new double[nres];
        for (int i = 1; i <= ntask; i++) {
            JobsNumber[i] = i;
        }

        SelectedJob = 0;
        double ColMin, RowMin;
        // code for min min scheduling
        for (int k = ntask; k > 0; k--) {
            // for (int j = 0; j <= k; j++)
            {
                FindResMinSuffrage(k);
                SelectedJob = FindBestResSufferage(k);
                System.out.println("Res:" + (int) MinOfCols[1][SelectedJob] + "-->job:" + JobsNumber[SelectedJob] + "\n");
                MinOfMins=ETC[(int) MinOfCols[1][SelectedJob]][JobsNumber[SelectedJob]];
                AddJobTimeToRes(MinOfMins, k, (int) MinOfCols[1][SelectedJob]);
                ReplaceColumns(k, SelectedJob);
                //PrintTMPETCArray();
            }// inner for    

        } // outer for 
        System.out.println("-----------------------------------------------------------------------------\n");
        MakeSpan = CalcMakeSpan();
        System.out.println("Makespan: " + MakeSpan);
        Utilization = CalcUtilization(MakeSpan);
        System.out.println("Utilization: " + Double.valueOf(decimal.format(Utilization * 100)) + " %\n\n");
    }

    /**
     * Genetic scheduling
     *
     */
    private void genetic() {
        int iterations, InitialStateNum;
        double MakeSpan1[] = new double[1];
        double MakeSpan2[] = new double[1];
        int Parent1Index, Parent2Index;
        Scanner sc = new Scanner(System.in);
        System.out.println("Please Enter iterations: ");
        iterations = sc.nextInt();
        System.out.println("Please Enter number of Initial states: ");
        InitialStateNum = sc.nextInt();
        Chromosome1 = new double[ntask + 1];
        double CopyChromosome1[] = new double[ntask + 1];
        Chromosome2 = new double[ntask + 1];
        double CopyChromosome2[] = new double[ntask + 1];
        DecimalFormat decimal = new DecimalFormat("#.##");

        Solution = new double[InitialStateNum][ntask + 1];
        System.out.println("\nIntitial Population:");
        GenerateInitialState(InitialStateNum);
        int cnt = 0;
        Parent1Index = FindBestSolution(InitialStateNum);
        Parent2Index = FindSecondBestSolution(InitialStateNum);
        CopyChromosomes(Parent1Index, Parent2Index);

        while (cnt < iterations) {
            CopyLastChromosomes(CopyChromosome1, CopyChromosome2);
            //CrossOver();
            Mutation();
            EvaluateChromosome(MakeSpan1, MakeSpan2);

            if (Chromosome1[ntask] > CopyChromosome1[ntask]) {
                for (int i = 0; i <= ntask; i++) {
                    Chromosome1[i] = CopyChromosome1[i];
                }
            }
            if (Chromosome2[ntask] > CopyChromosome2[ntask]) {
                for (int i = 0; i <= ntask; i++) {
                    Chromosome2[i] = CopyChromosome2[i];
                }
            }
            if (Chromosome1[ntask] < Chromosome2[ntask] && cnt == iterations - 1) {
                System.out.println("\nFinal Solution: ");
                for (int i = 0; i <= ntask; i++) {
                    System.out.print("\t" + Chromosome1[i]);
                }
                System.out.print("\n");
                System.out.println("\nMakeSpan: " + Chromosome1[ntask]);
                double Utilization = 0;
                double temp = 0;
                for (int i = 0; i < ntask; i++) {
                    temp += ETC[(int) Chromosome1[i]][i];
                }
                Utilization = (temp / (nres * Chromosome1[ntask])) * 100;
                System.out.println("Utilization: " + Double.valueOf(decimal.format( Utilization))+" %");
            } else if (Chromosome1[ntask] > Chromosome2[ntask] && cnt == iterations - 1) {
                System.out.println("\nFinal Solution: ");
                for (int i = 0; i <= ntask; i++) {
                    System.out.print("\t" + Chromosome1[i]);
                }
                System.out.println("\nMakeSpan: " + Chromosome2[ntask]);
                double Utilization = 0;
                double temp = 0;
                for (int i = 0; i < ntask; i++) {
                    temp += ETC[(int) Chromosome2[i]][i];
                }
                Utilization = (temp / (nres * Chromosome2[ntask])) * 100;
                System.out.println("Utilization: " +Double.valueOf(decimal.format( Utilization))+" %");
            }

            cnt++;
        }
        int sum;
        double Utilization[] = new double[nres];
    }

    public double FindResMin(int k) {
        MinOfCols = new double[2][k];
        double min;
        int IndexOfMin = 0;
        for (int j = 0; j < k; j++) {
            min = TMPETC[0][j];
            for (int i = 0; i < nres; i++) {
                if (min >= TMPETC[i][j]) {
                    min = TMPETC[i][j];
                    IndexOfMin = i;
                }
            }
            MinOfCols[0][j] = min;
            MinOfCols[1][j] = IndexOfMin;
        }
        return 0;
    }

    private int FindBestRes(int k) {
        int IndexOfMinJob = 0;
        MinOfMins = MinOfCols[0][0];
        for (int i = 0; i < k; i++) {
            if (MinOfMins > MinOfCols[0][i]) {
                MinOfMins = MinOfCols[0][i];
                IndexOfMinJob = i;
            }
        }
        // System.out.println("Min:"+MinOfMins);
        return IndexOfMinJob;
    }

    private int FindBestResMax(int k) {
        int IndexOfMaxJob = 0;
        MaxOfMins = MinOfCols[0][0];
        for (int i = 0; i < k; i++) 
        {
            if (MaxOfMins <= MinOfCols[0][i]) 
            {
                MaxOfMins = MinOfCols[0][i];
                IndexOfMaxJob = i;
            }
        }
       // System.out.println("MaxOfMinimums: "+MaxOfMins);
        return IndexOfMaxJob;
    }

    private void AddJobTimeToRes(double MinOfMins, int k, int ResNum) {
        DecimalFormat df = new DecimalFormat("#.######");
        //System.out.println("Value: " + df.format(value));
        for (int i = 0; i < k; i++) {
            TMPETC[ResNum][i] = Double.valueOf(df.format(TMPETC[ResNum][i] + MinOfMins));
            // System.out.println("\nvalue:"+TMPETC[ResNum][i]);
        }
        TMPETC[ResNum][ntask] = Double.valueOf(df.format(ETC[ResNum][JobsNumber[SelectedJob]] + TMPETC[ResNum][ntask]));
    }

    private void ReplaceColumns(int k, int SelectedJobIndex) {
        JobsNumber[SelectedJobIndex] = JobsNumber[k - 1];
        //System.out.println("index:"+SelectedJobIndex);
        for (int i = 0; i < nres; i++) {
            TMPETC[i][SelectedJobIndex] = TMPETC[i][k - 1];
        }
        for (int i = 0; i < nres; i++) {
            TMPETC[i][k - 1] = 0.0;
        }
    }

    private void PrintTMPETCArray() {
        for (int i = 0; i < nres; i++) {
            for (int j = 0; j < ntask + 1; j++) {
                System.out.print("\t" + TMPETC[i][j]);
            }
            System.out.print("\n");
        }
    }

    public double FindResMinSuffrage(int k) {
        MinOfCols = new double[4][k];
        double FirstMin, SecondMin;
        int IndexOfFirstMin = 0, IndexOfSecondMin = 0;
        for (int j = 0; j < k; j++) {
            SecondMin = FirstMin = 9.99999999999E20;
            for (int i = 0; i < nres; i++) {
                if (FirstMin > TMPETC[i][j]) {
                    SecondMin = FirstMin;
                    IndexOfSecondMin = IndexOfFirstMin;
                    FirstMin = TMPETC[i][j];
                    IndexOfFirstMin = i;
                } else if (TMPETC[i][j] < SecondMin && SecondMin != FirstMin) {
                    SecondMin = TMPETC[i][j];
                    IndexOfSecondMin = i;
                }
                // else if (SecondMin==FirstMin)
                // {
                //     SecondMin = TMPETC[i][j];
                //     IndexOfSecondMin=i;
                // }
            }
            MinOfCols[0][j] = FirstMin;
            MinOfCols[1][j] = IndexOfFirstMin;
            MinOfCols[2][j] = SecondMin;
            MinOfCols[3][j] = IndexOfSecondMin;

        }

        // for (int j=0;j<k;j++)
        // {
        //    System.out.print( "\tfirst min"+MinOfCols[0][j]); 
        //   System.out.print( "\tfirst min index"+MinOfCols[1][j]);
        //   System.out.print( "\tsecond min"+MinOfCols[2][j]); 
        //  System.out.print( "\tsecond min index"+MinOfCols[3][j]);
        // }
        return 0;
    }

    private int FindBestResSufferage(int k) {
        int IndexOfSelectedJob;
        double max;
        int IndexOfMax = 0;
        DecimalFormat df = new DecimalFormat("#.######");
        DifferenceOfMins = new double[2][k];
        for (int i = 0; i < k; i++) {
            DifferenceOfMins[0][i] = Double.valueOf(df.format(MinOfCols[2][i] - MinOfCols[0][i]));
            DifferenceOfMins[1][i] = MinOfCols[1][i];
            // System.out.print("\t"+DifferenceOfMins[0][i]);
        }
        max = DifferenceOfMins[0][0];
        for (int i = 1; i < k; i++) {
            if (DifferenceOfMins[0][i] > max) {
                max = DifferenceOfMins[0][i];
                IndexOfMax = i;
            }
        }
        //System.out.println("\nMax: "+max);
        // System.out.println("Max Index: "+IndexOfMax);
        MinOfMins = MinOfCols[0][IndexOfMax];
        //System.out.println("\nMin: "+MinOfMins);
        return IndexOfMax;
    }

    private double CalcMakeSpan() {
        double MakeSpan = TMPETC[0][ntask];
        for (int i = 0; i < nres; i++) {
            if (TMPETC[i][ntask] > MakeSpan) {
                MakeSpan = TMPETC[i][ntask];
            }
        }
        return MakeSpan;
    }

    private double CalcUtilization(double makespan) {
        double Sum = 0, temp, utilization;
        for (int i = 0; i < nres; i++) {
            Sum += TMPETC[i][ntask];
        }
        temp = nres * makespan;
        utilization = Sum / temp;
        return utilization;
    }

    private void GenerateInitialState(int InitialStateNum) {
        DecimalFormat decimal = new DecimalFormat("#.######");
        double MakeSpan = 0;
        Random rand = new Random();
        for (int k = 0; k < InitialStateNum; k++) {
            double MakeSpans[][] = new double[nres][ntask + 1];
            MakeSpan = 0;
            for (int j = 0; j < ntask; j++) {
                int i = rand.nextInt(nres);
                Solution[k][j] = i;
                MakeSpans[i][j] = ETC[i][j];
                //MakeSpan += ETC[i][j];
                System.out.print("\t" + Solution[k][j]);
            }
            for (int j = 0; j < nres; j++) {
                for (int t = 0; t < ntask; t++) {
                    MakeSpans[j][ntask] += MakeSpans[j][t];
                }
            }
            double mkspn = 0;
            for (int j = 0; j < nres; j++) {
                if (MakeSpans[j][ntask] > mkspn) {
                    mkspn = MakeSpans[j][ntask];
                }
            }
            MakeSpan = mkspn;
            // System.out.println("\nMakeSpan:"+ MakeSpan);
            Solution[k][ntask] = Double.valueOf(decimal.format(MakeSpan));
            System.out.print("\t" + Solution[k][ntask] + "\n");
        }
    }

    private int FindBestSolution(int InitialStateNum) {
        int BestSolutionIndex = 0;
        double BestSolution = 9.9999999999999E20;
        for (int i = 0; i < InitialStateNum; i++) {
            if (Solution[i][ntask] < BestSolution) {
                BestSolution = Solution[i][ntask];
                BestSolutionIndex = i;
            }
        }
        //System.out.println("Best Solution: "+ BestSolution+" index"+BestSolutionIndex);
        return BestSolutionIndex;
    }

    private int FindSecondBestSolution(int InitialStateNum) {
        double SecondBestSolution = 9.99999999999E20, BestSolution = 9.99999999999E20;
        int SecondBestSolutionIndex = 0;
        for (int i = 0; i < InitialStateNum; i++) {
            if (Solution[i][ntask] < BestSolution) {
                SecondBestSolution = BestSolution;
                //SecondBestSolutionIndex=i;
                BestSolution = Solution[i][ntask];
            } else if (Solution[i][ntask] < SecondBestSolution && Solution[i][ntask] != BestSolution) {
                SecondBestSolution = Solution[i][ntask];
                SecondBestSolutionIndex = i;
            }
        }
        // System.out.println("Best Solution: "+ BestSolution);
        // System.out.println("second Best Solution: "+ SecondBestSolution+" index"+SecondBestSolutionIndex);
        return SecondBestSolutionIndex;

    }

    private void CopyChromosomes(int BestSolutionIndex, int SecondBestSolutionIndex) {
        for (int i = 0; i <= ntask; i++) {
            Chromosome1[i] = Solution[BestSolutionIndex][i];
            Chromosome2[i] = Solution[SecondBestSolutionIndex][i];
        }
    }

    private void CrossOver() {
        Random rand = new Random();
        //int CrossOverPoint=rand.nextInt(ntask);
        int CrossOverPoint = ntask / 2;
        System.out.println("CrossOverPoint:" + CrossOverPoint);
        double TempArr[] = new double[ntask + 1];
        for (int i = 0; i < CrossOverPoint; i++) {
            TempArr[i] = Chromosome1[i];
            Chromosome1[i] = Chromosome2[i];
            Chromosome2[i] = TempArr[i];
        }
    }

    private void CopyLastChromosomes(double CopyChromosome1[], double CopyChromosome2[]) {
        for (int i = 0; i <= ntask; i++) {
            CopyChromosome1[i] = Chromosome1[i];
            CopyChromosome2[i] = Chromosome2[i];
        }
    }

    private void ReturnToLastChromosome(double CopyChromosome1[], double CopyChromosome2[]) {
        for (int i = 0; i <= ntask; i++) {
            Chromosome1[i] = CopyChromosome1[i];
            Chromosome1[i] = CopyChromosome2[i];
        }
    }

    private double EvaluateChromosome(double[] MakeSpan1, double[] MakeSpan2) {
        DecimalFormat decimal = new DecimalFormat("#.######");
        int i;
        double MakeSpans[][] = new double[nres][ntask + 1];
        MakeSpan1[0] = 0;
        MakeSpan2[0] = 0;
        for (int j = 0; j < ntask; j++) {
            i = (int) Chromosome1[j];
            MakeSpans[i][j] = ETC[i][j];
        }
        for (int p = 0; p < nres; p++) {
            for (int t = 0; t < ntask; t++) {
                MakeSpans[p][ntask] += MakeSpans[p][t];
            }
        }
        double mkspn = 0;
        for (int p = 0; p < nres; p++) {
            if (MakeSpans[p][ntask] > mkspn) {
                mkspn = MakeSpans[p][ntask];
            }
        }
        MakeSpan1[0] = mkspn;
        MakeSpans = new double[nres][ntask + 1];
        for (int j = 0; j < ntask; j++) {
            i = (int) Chromosome2[j];
            MakeSpans[i][j] = ETC[i][j];
        }
        for (int p = 0; p < nres; p++) {
            for (int t = 0; t < ntask; t++) {
                MakeSpans[p][ntask] += MakeSpans[p][t];
            }
        }
        mkspn = 0;
        for (int p = 0; p < nres; p++) {
            if (MakeSpans[p][ntask] > mkspn) {
                mkspn = MakeSpans[p][ntask];
            }
        }
        MakeSpan2[0] = mkspn;

        Chromosome1[ntask] = Double.valueOf(decimal.format(MakeSpan1[0]));
        Chromosome2[ntask] = Double.valueOf(decimal.format(MakeSpan2[0]));
        //System.out.println("\nMakeSpan: "+MakeSpan1[0]);
        // System.out.println("MakeSpan: "+MakeSpan2[0]);
        return 0;
    }

    private void Mutation() {
        int LastValue;
        int RandomTask, RandomRes;
        Random rand = new Random();
        RandomTask = rand.nextInt(ntask);
        RandomRes = rand.nextInt(nres);
        LastValue = (int) Chromosome1[RandomTask];
        Chromosome1[RandomTask] = RandomRes;
        RandomTask = rand.nextInt(ntask);
        RandomRes = rand.nextInt(nres);
        // LastValue=(int)Chromosome1[RandomTask];
        Chromosome2[RandomTask] = RandomRes;
    }

    /**
     * main
     *
     */
    public static void main(String[] args) throws Exception {
        int res, task;
        String fn;
        Scanner sc = new Scanner(System.in);
        if (args.length == 1) {
            fn = args[0];
        } else {
            fn="Data\\u_c_hihi.0";
        }
        Scheduling sched = new Scheduling();
        OUTER:
        while (true) {
            System.out.println("\n-----------------------------------------------------------------\nPlease Select One Algorithm:\n1-MinMin\n2-MaxMin\n3-Suffrage\n4-Genetic\n5-Exit");
            int algorithm = sc.nextInt();
            sched.read_etc(fn);
            switch (algorithm) {
                case 1:
                    sched.minmin();
                    break;
                case 2:
                    sched.maxmin();
                    break;
                case 3:
                    sched.suffrage();
                    break;
                case 4:
                    sched.genetic();
                    break;
                default:
                    break OUTER;
            }
        }
    }
}
