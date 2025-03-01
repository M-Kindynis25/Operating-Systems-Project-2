// laxen.cpp
#include <iostream>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/stat.h>  
#include <fcntl.h>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <iomanip>
#include "list.hpp"
#include "vector.hpp"

// Δομή Παραμέτρων
struct Parameters {
    char inputFile[256];
    char outputFile[256];
    char exclusionFile[256];
    int numOfSplitter;
    int numOfBuilders;
    int topK;
};

// Δομή για αποθήκευση file descriptors ενός pipe
struct PipeFD {
    int fd[2];
};

// Δομή που χρησιμοποιείται για την αποθήκευση μιας λέξης και του πλήθους εμφάνισής της
struct WordCount {
    char word[64];
    int count;
};

// Δομή για την αποθήκευση χρόνων εκτέλεσης
struct Time {
    double real_time;
    double cpu_time;
};

// Δομή αποτελεσμάτων για USR σήματα
struct USRResult {
    int received;       // Αριθμός ληφθέντων μηνυμάτων
    Vector<Time> times; // Χρόνοι για κάθε splitter ή builder
};

// Ανάλυση των ορισμάτων γραμμής εντολών
Parameters parseArguments(int argc, char* argv[]); 

// Μετράει τον αριθμό των γραμμών σε ένα αρχείο
int countLines(const char* filename);

// Συνάρτηση που μετατρέπει έναν ακέραιο σε συμβολοσειρά
const char* intToStr(int number);

// Συνάρτηση αναμένει να λάβει ένα συγκεκριμένο μήνυμα μέσω pipes
USRResult waitUSR(int numOf, const List<PipeFD>& allpipeUSR, const char* acceptBuffer);

// Συνάρτηση που διαβάζει τις κορυφαίες λέξεις από πολλαπλούς builders μέσω pipes
Vector<WordCount> readTopK(int numOf);

// Πρότυπη συνάρτηση για την ταξινόμηση ενός Vector σε σχεση με το count
template <typename T, typename Compare>
void vector_sort(Vector<T>& vec, Compare comp);

// Εγγραφή αποτελεσμάτων στο αρχείο εξόδου 
void writeResultsToFile(const Parameters& params, const Vector<WordCount>& vecTopK);

// Εκτύπωση αποτελεσμάτων στο TTY
void printResults(const Vector<WordCount>& vecTopK, int topK, const USRResult& resultUSR1, const USRResult& resultUSR2);

int main(int argc, char* argv[]) {
    // Αρχικοποίηση ονομάτων αρχείων και παραμέτρων
    Parameters params = parseArguments(argc, argv);

    Vector<char*> nameStoB;     // Αποθήκευση ονομάτων των named pipes από splitter προς builder
    for (int ii = 0; ii < params.numOfSplitter; ii++) { 
        for (int jj = 0; jj < params.numOfBuilders; jj++) { 
            char fifo_path[50];
            sprintf(fifo_path, "fifo_splitter%d_builder%d", ii, jj);
            nameStoB.push_back(fifo_path);

            // Δημιουργία του named pipe
            if (mkfifo(fifo_path, 0666) == -1) {
                if (errno != EEXIST) { // Αν το pipe υπάρχει ήδη, δεν είναι σφάλμα
                    std::perror("mkfifo");
                    return 2;
                }
            }
        }
    }

    Vector<char*> nameBtoL;     // Αποθήκευση ονομάτων των named pipes από builder προς laxen
    for (int jj = 0; jj < params.numOfBuilders; jj++) { 
        char fifo_path[50];
        sprintf(fifo_path, "fifo_builder%d_laxen", jj);
        nameBtoL.push_back(fifo_path);

        // Δημιουργία του named pipe
        if (mkfifo(fifo_path, 0666) == -1) {
            if (errno != EEXIST) { // Αν το pipe υπάρχει ήδη, δεν είναι σφάλμα
                std::perror("mkfifo");
                return 2;
            }
        }
    }

    pid_t pid;

    List<PipeFD> allpipeUSR2;  // Λίστα για αποθήκευση pipes επικοινωνίας USR2

    for (int i = 0; i < params.numOfBuilders; i++) {
        PipeFD pipe_USR2;
        if (pipe(pipe_USR2.fd) == -1) {     // Δημιουργία pipe για επικοινωνία USR2
            perror("pipe");
            exit(1);
        }
        allpipeUSR2.add(pipe_USR2);     // Προσθήκη του pipe στη λίστα

        // Εκκίνηση του builder
        pid = fork();
        if (pid < 0) {
            std::perror("fork");
            return 3;
        } else if (pid == 0) {  // Διαδικασία παιδιού
            close(pipe_USR2.fd[0]);  // Κλείνουμε το read end του pipe USR2

            // Εκτέλεση του builder μέσω execl
                execl("./builder",
                "builder",    
                "-p", intToStr(pipe_USR2.fd[1]),     
                "-id", intToStr(i), 
                "-l", intToStr(params.numOfSplitter), 
                "-t", intToStr(params.topK),       
                (char*)NULL); 
            // Αν η exec αποτύχει
            std::perror("execl");
            return 3;
        }
    }
    
    // Υπολογισμός του συνολικού αριθμού γραμμών του αρχείου
    int totalLines = countLines(params.inputFile);
    if (totalLines < 0) {  // Έλεγχος αποτυχίας ανάγνωσης
        std::perror("Error: Could not count lines in input file.");
        return 1;
    }
    // Υπολογισμός του αριθμού γραμμών ανά splitter
    int linesPerSplitter = totalLines / params.numOfSplitter;

    List<PipeFD> allpipeUSR1;       // Λίστα με pipes για επικοινωνία με τους splitters

    for (int i = 0; i < params.numOfSplitter; i++) {
        int startLine = i * linesPerSplitter;       // Υπολογισμός αρχικής γραμμής για τον splitter
        int endLine = (i == params.numOfSplitter - 1) ? totalLines - 1 : (startLine + linesPerSplitter - 1);    // Τελευταία γραμμή για τον τελευταίο splitter

        PipeFD pipe_USR1;
        if (pipe(pipe_USR1.fd) == -1) {
            perror("pipe");
            exit(1);
        }
        allpipeUSR1.add(pipe_USR1);     // Προσθήκη του pipe στη λίστα

        // Εκκίνηση του splitter
        pid = fork();
        if (pid < 0) { // Έλεγχος αποτυχίας fork
            std::perror("fork");
            return 3;
        } else if (pid == 0) {  // Διαδικασία παιδιού
            close(pipe_USR1.fd[0]);  // Κλείσιμο του read end του pipe στην παιδική διεργασία

            // Εκτέλεση του splitter μέσω execl
            execl("./splitter",
                "splitter",  
                "-p", intToStr(pipe_USR1.fd[1]) ,   
                "-id", intToStr(i),        
                "-i", params.inputFile,        
                "-e", params.exclusionFile,    
                "-m", intToStr(params.numOfBuilders),   
                "-sL", intToStr(startLine),      
                "-eL", intToStr(endLine),        
                (char*)NULL);  
            // Αν η exec αποτύχει
            std::perror("execl");
            return 3;
        }
    }

    // Αναμένω όλα τα Splitter να τελειώσουν
    Vector<Time> timesUSR1;
    USRResult resultUSR1 = waitUSR(params.numOfSplitter, allpipeUSR1, "SplitterDone");
    // Περιμένει να λάβει το μήνυμα "SplitterDone" από όλους τους splitters μέσω των pipes allpipeUSR1.

    // Διαβάζει τις κορυφαίες λέξεις από τους builders
    Vector<WordCount> vecTopK = readTopK(params.numOfBuilders);

    // Αναμένω όλα τα Builders να τελειώσου
    Vector<Time> timesUSR2;
    USRResult resultUSR2 = waitUSR(params.numOfBuilders, allpipeUSR2, "BuilderDonee");
    // Περιμένει να λάβει το μήνυμα "BuilderDonee" από όλους τους builders μέσω των pipes allpipeUSR2.

    // Ταξινόμηση με βάση το count σε φθίνουσα σειρά
    vector_sort(vecTopK, [](const WordCount& a, const WordCount& b) {
        return a.count > b.count;  
    });

    // Γράψιμο των αποτελεσμάτων στο αρχείο
    writeResultsToFile(params, vecTopK);


    // Η γονική διαδικασία περιμένει όλους τους συγγραφείς (splitters και builders) να ολοκληρωθούν
    for (int i = 0; i < params.numOfSplitter + params.numOfBuilders; ++i) {
        wait(NULL);     // Περιμένει την ολοκλήρωση όλων των child διεργασιών
    }

    // Εκτύπωση αποτελεσμάτων
    printResults(vecTopK, params.topK, resultUSR1, resultUSR2);
        
    return 0;
}

Parameters parseArguments(int argc, char* argv[]) {
    Parameters params = {"", "", "", 0, 0, 0};

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-i") == 0 && i + 1 < argc) {
            strncpy(params.inputFile, argv[i + 1], 255);
            params.inputFile[255] = '\0';
            i++;
        } else if (strcmp(argv[i], "-o") == 0 && i + 1 < argc) {
            strncpy(params.outputFile, argv[i + 1], 255);
            params.outputFile[255] = '\0';
            i++;
        } else if (strcmp(argv[i], "-e") == 0 && i + 1 < argc) {
            strncpy(params.exclusionFile, argv[i + 1], 255);
            params.exclusionFile[255] = '\0';
            i++;
        } else if (strcmp(argv[i], "-l") == 0 && i + 1 < argc) {
            params.numOfSplitter = std::atoi(argv[i + 1]);
            i++;
        } else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc) {
            params.numOfBuilders = std::atoi(argv[i + 1]);
            i++;
        } else if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) {
            params.topK = std::atoi(argv[i + 1]);
            i++;
        }
    }

    if (strlen(params.inputFile) == 0 || strlen(params.outputFile) == 0 || strlen(params.exclusionFile) == 0 ||
        params.numOfSplitter <= 0 || params.numOfBuilders <= 0 || params.topK <= 0) {
        std::cerr << "Usage: ./lexan -i inputfile -l numOfSplitter -m numOfBuilders -t TopPopular -e ExclusionList -o outfile" << std::endl;
        std::exit(1);
    }

    return params;
}

int countLines(const char* filename) {
    // Άνοιγμα του αρχείου εισόδου
    int file_fd = open(filename, O_RDONLY);
    if (file_fd == -1) {    // Έλεγχος αποτυχίας ανοίγματος
        std::perror("open input file");
        return -1;
    }

    // Μετατροπή του file descriptor σε FILE* για χρήση με fgets
    FILE* file = fdopen(file_fd, "r");
    if (file == NULL) {     // Έλεγχος αποτυχίας μετατροπής
        std::perror("fdopen");
        close(file_fd);
        return -1;
    }

    const size_t buffer_size = 1024;
    char buffer[buffer_size];

    int  line = 0;      // Μετρητής γραμμών
    while (fgets(buffer, buffer_size, file) != NULL) {  // Ανάγνωση γραμμών
        line++;
    }

    fclose(file);  // Κλείσιμο του αρχείου
    return line;   // Επιστροφή του αριθμού γραμμών
}

const char* intToStr(int number) {
    static char buffers[10][20];
    static int index = 0;

    char* str = buffers[index];
    index = (index + 1) % 10; 

    sprintf(str, "%d", number);
    return str;
}

USRResult waitUSR(int numOf, const List<PipeFD>& allpipeUSR, const char* acceptBuffer) {
    USRResult result;
    result.received = 0;       // Μετρητής ληφθέντων μηνυμάτων
    // Προσαρμογή μεγέθους του Vector
    for (int i = 0; i < numOf; ++i) {
        Time t;
        result.times.push_back(t); 
    }

    // Κλείνουμε τα write ends των pipes στον parent process
    List<PipeFD>::ListNode* currentNode = allpipeUSR.getHead();
    while (currentNode != nullptr) {
        close(currentNode->data.fd[1]);     // Κλείσιμο του write end κάθε pipe
        currentNode = currentNode->next;
    }

    // Δημιουργούμε έναν buffer για την αποθήκευση των μηνυμάτων που λαμβάνονται
    const size_t buffer_size = 1024;
    char buffer[buffer_size];

    // Διατρέχουμε τη λίστα των pipes για ανάγνωση
    currentNode = allpipeUSR.getHead();
    while (currentNode != nullptr && result.received < numOf) {
        int fd = currentNode->data.fd[0];   // Διαβάζουμε από το read end του pipe
        ssize_t bytesRead = read(fd, buffer, buffer_size - 1);
        if (bytesRead > 0) {
            buffer[bytesRead] = '\0'; // Τερματίζουμε τη συμβολοσειρά

            // Έλεγχος αν το μήνυμα ξεκινάει με το acceptBuffer
            if (strncmp(buffer, acceptBuffer, strlen(acceptBuffer)) == 0) {
                int id;
                double real_time, cpu_time;

                // Εξαγωγή των `id`, `real_time`, και `cpu_time` τιμών από τη συμβολοσειρά
                int res = sscanf(buffer + strlen(acceptBuffer), "-%d-%lf-%lf", &id, &real_time, &cpu_time);
                if (res == 3) { // Έλεγχος επιτυχίας
                    // Αποθήκευση στο vector στην κατάλληλη θέση
                    if (id >= 0 && static_cast<size_t>(id) < result.times.get_size()) {
                        result.times[id].real_time = real_time;
                        result.times[id].cpu_time = cpu_time;
                    } else {
                        std::cerr << "Invalid Splitter ID: " << id << std::endl;
                    }
                    result.received++;
                } else {
                    std::cerr << "Failed to parse message: " << buffer << std::endl;
                }
            }
        } else if (bytesRead == 0) {
            perror("Pipe closed without receiving ");
        } else {        // Σφάλμα κατά την ανάγνωση
            perror("read failed");
        }
        close(fd);      // Κλείσιμο του read end του pipe
        currentNode = currentNode->next;
    }


    // Έλεγχος αν τα αναμενόμενα μηνύματα λήφθηκαν
    if (result.received != numOf) {
        std::cerr << "Expected " << numOf << " " << acceptBuffer << " messages, but received " << result.received << std::endl;
    }

    return result;
}

Vector<WordCount> readTopK(int numOfBuilders) {
    // Vector για αποθήκευση των file descriptors από τα pipes των builders
    Vector<int> allpipeTOPK;    
    for (int jj = 0; jj < numOfBuilders; jj++) { 
        char fifo_path[50];
        sprintf(fifo_path, "fifo_builder%d_laxen", jj);     // Δημιουργία ονόματος του named pipe

        // Άνοιγμα του named pipe για ανάγνωση 
        int fd = open(fifo_path, O_RDONLY);
        if (fd == -1) {     // Έλεγχος αποτυχίας ανοίγματος
            std::perror("open");
            std::exit(EXIT_FAILURE);  
        }
        allpipeTOPK.push_back(fd);      // Αποθήκευση του file descriptor
    }

    // Vector για αποθήκευση των αποτελεσμάτων (WordCount)
    Vector<WordCount> wordCounts;

    const size_t buffer_size = 1024;
    char buffer[buffer_size];

    // Vector για παρακολούθηση των ενεργών FIFOs
    Vector<bool> activeFds;
    for (int i = 0; i < numOfBuilders; i++) activeFds.push_back(true);
    int activeCount = numOfBuilders;        // Μετρητής ενεργών FIFOs

    // Επεξεργασία συνεχίζεται όσο υπάρχουν ενεργά FIFOs
    while (activeCount > 0) {
        for (int i = 0; i < numOfBuilders; i++) { 
            if (activeFds[i]) {     // Ελέγχουμε αν το FIFO είναι ακόμα ενεργό
                ssize_t bytes_read = read(allpipeTOPK[i], buffer, buffer_size - 1);
                if (bytes_read == -1) {     // Σφάλμα ανάγνωσης
                    std::perror("read");
                    std::exit(EXIT_FAILURE);  
                }
                if (bytes_read == 0) {      // EOF, κλείσιμο του FIFO
                    activeFds[i] = false;
                    activeCount--;
                    break; 
                }

                buffer[bytes_read] = '\0';      // Εξασφαλίζουμε τερματισμό της συμβολοσειράς

                // Διαχωρισμός λέξεων χρησιμοποιώντας strtok
                char* token = strtok(buffer, " \n");
                while (token != NULL) {
                    // Επεξεργασία της λέξης 
                    WordCount wc;

                    // Βρίσκουμε τη θέση του '-'
                    char* hyphen = strchr(token, '-');
                    if (hyphen != nullptr) {
                        *hyphen = '\0'; // Αντικαθιστούμε το '-' με '\0' για να τερματίσουμε τη λέξη

                        // Αντιγράφουμε τη λέξη στο wc.word
                        strncpy(wc.word, token, sizeof(wc.word));
                        wc.word[sizeof(wc.word) - 1] = '\0'; // Εξασφαλίζουμε ότι τερματίζεται με '\0'

                        // Μετατροπή του αριθμού (count) σε ακέραιο
                        wc.count = atoi(hyphen + 1);
                    } else {
                        // Διαχείριση σφάλματος: Δεν βρέθηκε '-'
                        std::perror("Error: Invalid token format.");
                        std::exit(EXIT_FAILURE);  
                    }

                    wordCounts.push_back(wc);        // Προσθήκη του WordCount στο vector αποτελεσμάτων


                    // Επεξεργασία επόμενης λέξης
                    token = strtok(NULL, " \n");
                }
            }
        }
    }

    // Κλείσιμο όλων των pipes
    for (size_t i = 0; i < allpipeTOPK.get_size(); i++) close(allpipeTOPK[i]);

    return wordCounts;       // Επιστροφή του vector με τα αποτελέσματα
}

template <typename T, typename Compare>
void vector_sort(Vector<T>& vec, Compare comp) {
    size_t n = vec.get_size();
    // Εφαρμογή αλγορίθμου ταξινόμησης
    for (size_t i = 0; i < n - 1; ++i) {
        for (size_t j = 0; j < n - i - 1; ++j) {
            // Χρήση της συνάρτησης σύγκρισης comp για να αποφασιστεί η σειρά
            if (comp(vec[j + 1], vec[j])) {
                // Ανταλλαγή των στοιχείων vec[j] και vec[j + 1]
                T temp = vec[j];
                vec[j] = vec[j + 1];
                vec[j + 1] = temp;
            }
        }
    }
}

void writeResultsToFile(const Parameters& params, const Vector<WordCount>& vecTopK) {
    // Άνοιγμα του αρχείου για εγγραφή
    int fd = open(params.outputFile, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        std::perror("open");
        return;
    }

    // Υπολογισμός του μέγιστου μήκους λέξης για στοίχιση
    int maxWordLength = 4;  // Ξεκινάμε με το μήκος της λέξης "Word"
    for (size_t i = 0; i < static_cast<size_t>(params.topK) && i < vecTopK.get_size(); i++) {
        int wordLength = std::strlen(vecTopK[i].word);
        if (wordLength > maxWordLength) {
            maxWordLength = wordLength;
        }
    }

    // Δημιουργία και εγγραφή της κεφαλίδας
    char header[256];
    int padding = maxWordLength - 4 + 5;  // 4 είναι το μήκος του "Word"
    snprintf(header, sizeof(header), "     Word%*sFrequency\n", padding, "");  // Προσθέτουμε κενά για στοίχιση
    write(fd, header, std::strlen(header));

    // Γραμμή διαχωρισμού
    int separatorLength = maxWordLength + 19;
    char separator[separatorLength + 1];
    memset(separator, '-', separatorLength);
    separator[separatorLength] = '\n';
    write(fd, separator, separatorLength + 1);

    // Εγγραφή των top K λέξεων με αριθμητική αρίθμηση
    for (size_t i = 0; i < static_cast<size_t>(params.topK) && i < vecTopK.get_size(); i++) {
        char line[512];
        padding = maxWordLength - std::strlen(vecTopK[i].word) + 5;
        snprintf(line, sizeof(line), "%3lu. %s%*s%d\n", i + 1, vecTopK[i].word, padding, "", vecTopK[i].count);
        write(fd, line, std::strlen(line));
    }

    // Τελική γραμμή διαχωρισμού
    write(fd, separator, separatorLength + 1);

    // Κλείσιμο του αρχείου
    close(fd);
}

void printResults(const Vector<WordCount>& vecTopK, int topK, const USRResult& resultUSR1, const USRResult& resultUSR2) {
    // Εκτύπωση κορυφαίων λέξεων
    std::cout << std::endl;
    std::cout << "     Word                  Frequency" << std::endl;
    std::cout << "------------------------------------" << std::endl;

    for (int i = 0; i < topK; ++i) {
        const WordCount& wc = vecTopK[i]; // Υποθέτουμε ότι WordCount έχει μέλη `word` και `count`
        std::cout << std::setw(4) << std::right << i + 1 << ". "    // Αριθμός με δεξιά στοίχιση
                  << std::setw(15) << std::left << wc.word          // Λέξη με αριστερή στοίχιση
                  << std::setw(10) << std::right << wc.count        // Συχνότητα με δεξιά στοίχιση
                  << std::endl;
    }

    std::cout << "------------------------------------" << std::endl;

    // Εκτύπωση αποτελεσμάτων splitters
    std::cout << std::endl;
    std::cout << "Splitter Results:" << std::endl;
    for (size_t i = 0; i < resultUSR1.times.get_size(); ++i) {
        std::cout << "Splitter " << i << " Real Time: " << resultUSR1.times[i].real_time
                  << ", CPU Time: " << resultUSR1.times[i].cpu_time << std::endl;
    }

    // Εκτύπωση αποτελεσμάτων builders
    std::cout << "Builder Results:" << std::endl;
    for (size_t i = 0; i < resultUSR2.times.get_size(); ++i) {
        std::cout << "Builder " << i << " Real Time: " << resultUSR2.times[i].real_time
                  << ", CPU Time: " << resultUSR2.times[i].cpu_time << std::endl;
    }

    // Εκτύπωση αριθμού σημάτων
    std::cout << std::endl;
    std::cout << "USR1 Signals Received: " << resultUSR1.received << std::endl;
    std::cout << "USR2 Signals Received: " << resultUSR2.received << std::endl;
}