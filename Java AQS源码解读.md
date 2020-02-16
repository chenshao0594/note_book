# Java AQS源码解读

### 1、先聊点别的

说实话，关于AQS的设计理念、实现、使用，我有打算写过一篇技术文章，但是在写完初稿后，发现掌握的还是模模糊糊的，模棱两可。
痛定思痛，脚踏实地重新再来一遍。这次以 Java 8源码为基础进行解读。

### 2、AQS简介

在`java.util.concurrent.locks`包下，有两个这样的类：

- **AbstractQueuedSynchronizer**
- **AbstractQueuedLongSynchronizer**

这两个类的唯一区别就是：

- AbstractQueuedSynchronizer内部维护的`state`变量是`int`类型
- AbstractQueuedLongSynchronizer内部维护的`state`变量是`long`类型

我们常说的`AQS`其实泛指的就是这两个类，即`抽象队列同步器`。

抽象队列同步器AbstractQueuedSynchronizer （以下都简称AQS），是用来构建锁或者其他同步组件的骨架类，减少了各功能组件实现的代码量，也解决了在实现同步器时涉及的大量细节问题，例如等待线程采用FIFO队列操作的顺序。在不同的同步器中还可以定义一些灵活的标准来判断某个线程是应该通过还是等待。

AQS采用模板方法模式，在内部维护了n多的模板的方法的基础上，子类只需要实现特定的几个方法（不是抽象方法！不是抽象方法！不是抽象方法！），就可以实现子类自己的需求。

基于AQS实现的组件，诸如：

- ReentrantLock 可重入锁（支持公平和非公平的方式获取锁）
- Semaphore 计数信号量
- ReentrantReadWriteLock 读写锁
- …

AQS是Doug Lea的大作之一，在维基百科查关于他的资料时，偶然发现老爷子喜欢红色或淡粉色衬衫？

### 3、AQS设计思路

AQS内部维护了一个int成员变量来表示同步状态，通过内置的FIFO(first-in-first-out)同步队列来控制获取共享资源的线程。

我们可以猜测出，AQS其实主要做了这么几件事情：

- 同步状态（state）的维护管理
- 等待队列的维护管理
- 线程的阻塞与唤醒

> ps: 当然了，其内部还维护了一个`ConditionObject` 内部类，主要用作线程的协作与通信，我们暂时先不讲这个帅哥。

通过AQS内部维护的int型的state，可以用于表示任意状态！

- ReentrantLock用它来表示锁的持有者线程已经重复获取该锁的次数，而对于非锁的持有者线程来说，如果state大于0，意味着无法获取该锁，将该线程包装为Node，加入到同步等待队列里。
- Semaphore用它来表示剩余的许可数量，当许可数量为0时，对未获取到许可但正在努力尝试获取许可的线程来说，会进入同步等待队列，阻塞，直到一些线程释放掉持有的许可（state+1），然后争用释放掉的许可。
- FutureTask用它来表示任务的状态（未开始、运行中、完成、取消）。
- ReentrantReadWriteLock在使用时，稍微有些不同，int型state用二进制表示是32位，前16位（高位）表示为读锁，后面的16位（低位）表示为写锁。
- CountDownLatch使用state表示计数次数，state大于0，表示需要加入到同步等待队列并阻塞，直到state等于0，才会逐一唤醒等待队列里的线程。

#### 3.1 伪代码之获取锁：

```
boolean acquire() throws InterruptedException {
  while(当前状态不允许获取操作) {
    if(需要阻塞获取请求) {
      如果当前线程不在队列中，则将其插入队列
      阻塞当前线程
    }
    else
      返回失败
  }
  可能更新同步器的状态
  如果线程位于队列中，则将其移出队列
  返回成功
}
复制代码
```

#### 3.2 伪代码之释放锁：

```
void release() {
  更新同步器的状态
  if (新的状态允许某个被阻塞的线程获取成功)
    解除队列中一个或多个线程的阻塞状态
}
复制代码
```

大概就是阐述这么个思路。

#### 3.3 提供的方法

##### 3.3.1 共通方法

以下三个方法，均为`protected final`修饰，每个继承AQS的类都可以调用这三个方法。

- protected final int getState() 获取同步状态
- protected final void setState(int newState) 设置同步状态
- protected final boolean compareAndSetState(int expect, int update) 如果当前状态值等于预期值，原子性地将同步状态设置为给定的更新值，并返回true；否则返回false

##### 3.3.2 子类需要实现的方法

以下五个方法，在AQS内部并未实现，而是交由子类去实现，然后AQS再调用子类的实现方法，完成逻辑处理。

- protected boolean tryAcquire(int) 尝试以独占模式获取操作，应查询对象的状态是否允许以独占模式获取它，如果允许则获取它。
- protected boolean tryRelease(int) 尝试释放同步状态
- protected int tryAcquireShared(int) 共享的方式尝试获取操作
- protected boolean tryReleaseShared(int) 共享的方式尝试释放
- protected boolean isHeldExclusively() 调用此方法的线程，是否是独占锁的持有者

子类无须实现上述的所有方法，可以选择其中一部分进行覆写，但是要保持实现逻辑完整，不能穿插实现。根据实现方式不同，分为独占锁策略实现和共享锁策略实现。

> 这也是为什么上述方法没有定义为抽象方法的原因。如果定义为抽象方法，子类必须实现所有的五个方法，哪怕你压根就用不到。

**独占锁：**

- ReentrantLock
- ReentrantReadWriteLock.WriteLock
  实现策略：
- tryAcquire(int)
- tryRelease(int)
- isHeldExclusively()

**共享锁：**

- CountDownLatch
- ReentrantReadWriteLock.ReadLock
- Semaphore
  实现策略：
- tryAcquireShared(int)
- tryReleaseShared(int)

AQS还有很多内部模板方法，就不一一举例了，之后的源码解读，会展示一部分，并会配上骚气的注释。

### 4、AQS内部属性

#### 4.1 CLH队列

AQS通过内置的FIFO(first-in-first-out)同步队列来控制获取共享资源的线程。CLH队列是FIFO的双端双向队列，AQS的同步机制就是依靠这个CLH队列完成的。队列的每个节点，都有前驱节点指针和后继节点指针。

> **头结点并不在阻塞队列内！**

![AQS-Node.jpg](https://user-gold-cdn.xitu.io/2020/1/11/16f922162108de3b?imageView2/0/w/1280/h/960/format/webp/ignore-error/1)AQS-Node.jpg

Node源码：

```java
static final class Node {
    // 共享模式下等待标记
    static final Node SHARED = new Node();

    // 独占模式下等待标记
    static final Node EXCLUSIVE = null;

    // 表示当前的线程被取消
    static final int CANCELLED = 1;

    // 表示当前节点的后继节点包含的线程需要运行，也就是unpark
    static final int SIGNAL = -1;

    // 表示当前节点在等待condition，也就是在condition队列中
    static final int CONDITION = -2;

    // 表示当前场景下后续的acquireShared能够得以执行
    static final int PROPAGATE = -3;
    /**
     * CANCELLED =  1 // 当前线程因为超时或者中断被取消。这是一个终结态，也就是状态到此为止。
     * SIGNAL    = -1 // 表示当前线程的后继线程被阻塞或即将被阻塞，当前线程释放锁或者取消后需要唤醒后继线程。这个状态一般都是后继节点设置前驱节点的
     * CONDITION = -2 // 表示当前线程在Condition队列中
     * PROPAGATE = -3 // 用于将唤醒后继线程传递下去，这个状态的引入是为了完善和增强共享锁的唤醒机制
     * 0              // 表示无状态或者终结状态！
     */
    volatile int waitStatus;

    // 前驱节点
    volatile Node prev;

    // 后继节点
    volatile Node next;

    // 当前节点的线程，初始化使用，在使用后失效
    volatile Thread thread;

    // 存储condition队列中的后继节点
    Node nextWaiter;

    // 如果该节点处于共享模式下等待，返回true
    final boolean isShared() {
        return nextWaiter == SHARED;
    }
    // 返回当前节点的前驱节点，如果为空，直接抛出空指针异常
    final Node predecessor() throws NullPointerException {
        Node p = prev;
        if (p == null)
            throw new NullPointerException();
        else
            return p;
    }

    Node() {    // Used to establish initial head or SHARED marker
    }

    // 指定线程和模式的构造方法
    Node(Thread thread, Node mode) {     // Used by addWaiter
        // SHARED和EXCLUSIVE 用于表示当前节点是共享还是独占
        this.nextWaiter = mode;
        this.thread = thread;
    }

    // 指定线程和节点状态的构造方法
    Node(Thread thread, int waitStatus) { // Used by Condition
        this.waitStatus = waitStatus;
        this.thread = thread;
    }
}
复制代码
```

#### 4.2 volatile state

最为重要的属性，这个整数可以用于表示任意状态！在上面有说过。

#### 4.2 volatile head & volatile tail

head 头结点，但是这个头节点只是个虚节点，只是逻辑上代表持有锁的线程节点，且head节点是不存储thread线程信息和前驱节点信息的。

tail 尾节点，每个新节点都会进入队尾。不存储后继节点信息。

- 这两个属性是延迟初始化的，在第一次且第一个线程持有锁时，第二个线程因为获取失败，进入同步队列时会对head和tail进行初始化，也就是说在所有线程都能获取到锁时，其内部的head和tail都为null，一旦head 和 tail被初始化后，即使后来没有线程持有锁，其内部的head 和 tail 依然保留最后一个持有锁的线程节点！（head 和 tail都指向一个内存地址）
- 当一个线程获取锁失败而被加入到同步队列时，会用CAS来设置尾节点tail为当前线程对应的Node节点。
- AQS内部的cas操作，都是依赖Unsafe类的，自Java9之后的版本，Unsafe类被移除，取而代之的是VarHandle类。

这两个属性均为volatile所修饰（保证了变量具有有序性和可见性）

#### 4.3 spinForTimeoutThreshold

自旋超时阀值，在doAcquireSharedNanos()等方法中有使用到。

- 如果用户定义的等待时间超过这个阀值，那么线程将阻塞，在阻塞期间如果能够等到唤醒的机会并tryAcquireShared成功，则返回true，否则返回false，超时也返回false。
- 如果用户定义的等待时间小于等于这个阀值，则会无限循环，线程不阻塞，直到有线程释放同步状态或者超时，然后返回对应的结果。

#### 4.4 exclusiveOwnerThread

这是AQS通过继承`AbstractOwnableSynchronizer`类，获得的属性，表示独占模式下的同步器持有者。

### 5、AQS具体实现

#### 5.1 独占锁实现思路

##### 5.1.1 获取锁 ReentrantLock.lock()

```
/**
 * 获取独占锁，忽略中断。
 * 首先尝试获取锁，如果成功，则返回true；否则会把当前线程包装成Node插入到队尾，在队列中会检测是否为head的直接后继，并尝试获取锁,
 * 如果获取失败，则会通过LockSupport阻塞当前线程，直至被释放锁的线程唤醒或者被中断，随后再次尝试获取锁，如此反复。被唤醒后继续之前的代码执行
 */
public final void acquire(int arg) {
    if (!tryAcquire(arg) && acquireQueued(addWaiter(Node.EXCLUSIVE), arg))
        selfInterrupt();
}
---------------------------------------------------------------------------------------
其中tryAcquire()方法需要由子类实现，ReentrantLock通过覆写这个方法实现了公平锁和非公平锁
---------------------------------------------------------------------------------------

/**
 * 在同步等待队列中插入节点
 */
private Node addWaiter(Node mode) {
    Node node = new Node(Thread.currentThread(), mode);
    Node pred = tail;
    // 判断尾节点是否为null
    if (pred != null) {
        node.prev = pred;
        // 通过CAS在队尾插入当前节点
        if (compareAndSetTail(pred, node)) {
            pred.next = node;
            return node;
        }
    }
    // tail节点为null，则将新节点插入队尾，必要时进行初始化
    enq(node);
    return node;
}

/**
 * 通过无限循环和CAS操作在队列中插入一个节点成功后返回。
 * 将节点插入队列，必要时进行初始化
 */
private Node enq(final Node node) {
    for (;;) {
        Node t = tail;
        // 初始化head和tail
        if (t == null) {
            if (compareAndSetHead(new Node()))
                tail = head;
        } else {
            node.prev = t;
            /*
             CAS设置tail为node
             表面上看是把老tail的next连接到node。
             如果同步队列head节点和tail节点刚刚被这个线程初始化，实际上也把head的next也连接到了node，而老tail节点被node取缔。
             反之则是，把老tail的next连接到node，head并没有与node产生连接，这样就形成了链表 head <-> old_tail <-> tail
             */
            if (compareAndSetTail(t, node)) {
                t.next = node;
                return t;
            }
        }
    }
}

/**
 * 在队列中的节点通过此方法获取锁，忽略中断。
 * 这个方法很重要，如果上述没有获取到锁，将线程包装成Node节点加入到同步队列的尾节点，然后看代码里的注释
 */
final boolean acquireQueued(final Node node, int arg) {
    boolean failed = true;
    try {
        boolean interrupted = false;
        for (;;) {
            final Node p = node.predecessor();
            /*
             * 检测当前节点前驱是否head，这是试获取锁。
             * 如果是的话，则调用tryAcquire尝试获取锁,
             * 成功，则将head置为当前节点。原head节点的next被置为null等待GC垃圾回收
             */
            if (p == head && tryAcquire(arg)) {
                setHead(node);
                p.next = null; // help GC
                failed = false;
                return interrupted;
            }
            /*
             * 如果未成功获取锁则根据前驱节点判断是否要阻塞。
             * 如果阻塞过程中被中断，则置interrupted标志位为true。
             * shouldParkAfterFailedAcquire方法在前驱状态不为SIGNAL的情况下都会循环重试获取锁。
             * 如果shouldParkAfterFailedAcquire返回true，则会将当前线程阻塞并检查是否被中断
             */
            if (shouldParkAfterFailedAcquire(p, node) && parkAndCheckInterrupt())
                interrupted = true;
        }
    } finally {
        if (failed)
            cancelAcquire(node);
    }
}

/**
 * 根据前驱节点中的waitStatus来判断是否需要阻塞当前线程。
 */
private static boolean shouldParkAfterFailedAcquire(Node pred, Node node) {
    int ws = pred.waitStatus;
    if (ws == Node.SIGNAL)
        /*
         * 前驱节点设置为SIGNAL状态，在释放锁的时候会唤醒后继节点，
         * 所以后继节点（也就是当前节点）现在可以阻塞自己。
         */
        return true;
    if (ws > 0) {
        /*
         * 前驱节点状态为取消,向前遍历，更新当前节点的前驱为往前第一个非取消节点。
         * 当前线程会之后会再次回到循环并尝试获取锁。
         */
        do {
            node.prev = pred = pred.prev;
        } while (pred.waitStatus > 0);
        pred.next = node;
    } else {
         /**
          * 等待状态为0或者PROPAGATE(-3)，设置前驱的等待状态为SIGNAL,
          * 并且之后会回到循环再次重试获取锁。
          */
        compareAndSetWaitStatus(pred, ws, Node.SIGNAL);
    }
    return false;
}

/**
 * 该方法实现某个node取消获取锁。
 */
private void cancelAcquire(Node node) {
   if (node == null)
       return;

   node.thread = null;

   // 遍历并更新节点前驱，把node的prev指向前部第一个非取消节点。
   Node pred = node.prev;
   while (pred.waitStatus > 0)
       node.prev = pred = pred.prev;

   // 记录pred节点的后继为predNext，后续CAS会用到。
   Node predNext = pred.next;

   // 直接把当前节点的等待状态置为取消,后继节点调用cancelAcquire方法时，也可以跨过该节点
   node.waitStatus = Node.CANCELLED;

   // 如果当前节点是尾节点，则将尾节点置为当前节点的前驱节点
   if (node == tail && compareAndSetTail(node, pred)) {
       compareAndSetNext(pred, predNext, null);
   } else {
       // 如果node还有后继节点，这种情况要做的是把pred和后继非取消节点拼起来。
       int ws;
       if (pred != head && ((ws = pred.waitStatus) == Node.SIGNAL || (ws <= 0 && compareAndSetWaitStatus(pred, ws, Node.SIGNAL))) && pred.thread != null) {
           Node next = node.next;
           /* 
            * 如果node的后继节点next非取消状态的话，则用CAS尝试把pred的后继置为node的后继节点
            * 这里if条件为false或者CAS失败都没关系，这说明可能有多个线程在取消，总归会有一个能成功的。
            */
           if (next != null && next.waitStatus <= 0)
               compareAndSetNext(pred, predNext, next);
       } else {
           unparkSuccessor(node);
       }

       /*
        * 在GC层面，和设置为null具有相同的效果
        */
       node.next = node; 
   }
}
复制代码
```

**获取独占锁的执行过程大致如下：**
假设当前锁已经被线程A持有，且持有锁的时间足够长（方便我们讲解，也防止抬杠），线程B、C获取锁失败。

线程B：

- 1、将线程B包装成Node节点（简称BN），加入到同步等待队列，此时BN的waitStatus=0
- 2、将tail节点设置为BN，且与head节点相连，形成链表
- 3、head节点是个虚拟节点，也就是持有锁的线程（但并不包含有线程信息），tail节点就是BN
- 4、线程B进入"无限循环"，判断前驱节点是否为头节点（true）并再次尝试获取锁（false，获取锁失败）
- 5、线程B将进入shouldParkAfterFailedAcquire方法，在方法内部，将BN的前驱节点（也就是头结点）的waitStatus设置为 -1，此方法返回false
- 6、因为是无限循环，所以线程B再次进入shouldParkAfterFailedAcquire方法，由于BN的前驱节点（也就是头结点）的waitStatus为 -1，所以直接返回true
- 7、调用parkAndCheckInterrupt，当前线程B被阻塞，等待唤醒。

线程C：

- 1、将线程C包装成Node节点（简称CN），加入到同步等待队列，此时CN的waitStatus=0
- 2、将tail节点设置为CN，且与原tail节点（BN节点）相连
- 3、线程C进入"无限循环"，判断前驱节点是否为头节点（false）
- 4、线程C将进入shouldParkAfterFailedAcquire方法，在方法内部，将CN的前驱节点（也就是BN结点）的waitStatus设置为 -1，此方法返回false
- 5、因为是无限循环，所以线程C再次进入shouldParkAfterFailedAcquire方法，由于CN的前驱节点（也就是BN结点）的waitStatus为 -1，所以直接返回true
- 6、调用parkAndCheckInterrupt，线程C被阻塞，等待唤醒。

最终的队列如下：

```
+------+        +------+        +------+
|      |  <---  |      |  <---  |      |
| head |        |  BN  |        | tail |
|  AN  |  --->  |      |  --->  | (CN) |
+------+        +------+        +------+
复制代码
```

##### 5.1.2 释放锁 ReentrantLock.unlock()

对于释放独占锁，会调用tryRelaes(int)方法，该方法由子类实现，在完全释放掉锁后，释放掉锁的线程会将后继线程唤醒，后继线程进行锁争用（非公平锁）

```
public final boolean release(int arg) {
    if (tryRelease(arg)) {
        Node h = head;
        // 头结点不为null且后继节点是需要被唤醒的
        if (h != null && h.waitStatus != 0)
            unparkSuccessor(h);
        return true;
    }
    return false;
}
复制代码
```

**释放独占锁的执行过程大致如下（假设有后继节点需要唤醒）：**

- 将head节点的`waitStatus`设置为0
- 唤醒后继节点
- 后继节点线程被唤醒后，会将后继节点设置为head，并对后继节点内的prev和thread属性设置为null
- 对原head节点的next指针设置为null，等待GC回收原head节点。

```
+------+        +------+        +------+
| old  |  <-X-  | new  |  <---  |      |
| head |        | head |        | tail |
|  AN  |  -X->  |  BN  |  --->  | (CN) |
+------+        +------+        +------+
复制代码
```

如上所示，AN节点（原head节点）等待被GC垃圾回收。

#### 5.2 共享锁实现思路

##### 5.2.1 获取锁

与获取独占锁不同，关键在于，共享锁可以被多个线程持有。

如果需要AQS实现共享锁，在实现tryAcquireShared()方法时：

- 返回负数，表示获取失败
- 返回0，表示获取成功，但是后继争用线程不会成功
- 返回正数，表示获取成功，表示后继争用线程也可能成功

```java
public final void acquireShared(int arg) {
    if (tryAcquireShared(arg) < 0)
        doAcquireShared(arg);
}

private void doAcquireShared(int arg) {
    final Node node = addWaiter(Node.SHARED);
    boolean failed = true;
    try {
        boolean interrupted = false;
        for (;;) {
            final Node p = node.predecessor();
            if (p == head) {
                int r = tryAcquireShared(arg);
                // 一旦共享获取成功，设置新的头结点，并且唤醒后继线程
                if (r >= 0) {
                    setHeadAndPropagate(node, r);
                    p.next = null; // help GC
                    if (interrupted)
                        selfInterrupt();
                    failed = false;
                    return;
                }
            }
            if (shouldParkAfterFailedAcquire(p, node) && parkAndCheckInterrupt())
                interrupted = true;
        }
    } finally {
        if (failed)
            cancelAcquire(node);
    }
}

/**
 * 这个函数做的事情有两件:
 * 1. 在获取共享锁成功后，设置head节点
 * 2. 根据调用tryAcquireShared返回的状态以及节点本身的等待状态来判断是否要需要唤醒后继线程
 */
private void setHeadAndPropagate(Node node, int propagate) {
    // 把当前的head封闭在方法栈上，用以下面的条件检查
    Node h = head;
    setHead(node);
    /*
     * propagate是tryAcquireShared的返回值，这是决定是否传播唤醒的依据之一
     * h.waitStatus为SIGNAL或者PROPAGATE时也根据node的下一个节点共享来决定是否传播唤醒
     */
    if (propagate > 0 || h == null || h.waitStatus < 0 || (h = head) == null || h.waitStatus < 0) {
        Node s = node.next;
        if (s == null || s.isShared())
            doReleaseShared();
    }
}

/**
 * 这是共享锁中的核心唤醒函数，主要做的事情就是唤醒下一个线程或者设置传播状态。
 * 后继线程被唤醒后，会尝试获取共享锁，如果成功之后，则又会调用setHeadAndPropagate,将唤醒传播下去。
 * 这个函数的作用是保障在acquire和release存在竞争的情况下，保证队列中处于等待状态的节点能够有办法被唤醒。
 */
private void doReleaseShared() {
    /*
     * 以下的循环做的事情就是，在队列存在后继线程的情况下，唤醒后继线程；
     * 或者由于多线程同时释放共享锁由于处在中间过程，读到head节点等待状态为0的情况下，
     * 虽然不能unparkSuccessor，但为了保证唤醒能够正确稳固传递下去，设置节点状态为PROPAGATE。
     * 这样的话获取锁的线程在执行setHeadAndPropagate时可以读到PROPAGATE，从而由获取锁的线程去释放后继等待线程。
     */
    for (;;) {
        Node h = head;
        // 如果队列中存在后继线程。
        if (h != null && h != tail) {
            int ws = h.waitStatus;
            if (ws == Node.SIGNAL) {
                if (!compareAndSetWaitStatus(h, Node.SIGNAL, 0))
                    continue;
                unparkSuccessor(h);
            }
            // 如果h节点的状态为0，需要设置为PROPAGATE用以保证唤醒的传播。
            else if (ws == 0 && !compareAndSetWaitStatus(h, 0, Node.PROPAGATE))
                continue;
        }
        // 检查h是否仍然是head，如果不是的话需要再进行循环。
        if (h == head)
            break;
    }
}
复制代码
```

##### 5.2.1 释放锁

释放共享锁与获取共享锁的代码都使用了doReleaseShared(int)

```java
public final boolean releaseShared(int arg) {
    if (tryReleaseShared(arg)) {
        // doReleaseShared的实现上面获取共享锁已经介绍
        doReleaseShared();
        return true;
    }
    return false;
}
复制代码
```

我觉得大家应该都能看懂，还是简单说一下吧（手动狗头～）：

同步等待队列中，在唤醒因为获取共享锁失败而阻塞的后继节点线程后，后继节点线程会依次唤醒其后继节点！依次类推。

**再换种说法？**

这种情况有可能是：写锁导致获取读锁的一些线程阻塞，而写锁释放后，会唤醒后继节点线程，如果该后继节点，恰好是因为获取读锁失败而阻塞的线程，那么该后继节点线程会唤醒其后继节点…直到全部获取读锁成功，或者某一节点获取写锁成功。

### 6、拓展

#### 6.1 不得不说的PROPAGATE

[有个关于AQS的bug，真的值得大家看一看](https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6801020)

在共享锁获取与释放的操作中，我觉得有个特别的重要的waitStatus状态值，要和大家说一说，就是`PROPAGATE`，这个属性值的意思是，用于将唤醒后继线程传递下去，这个状态的引入是为了完善和增强共享锁的唤醒机制。

之前翻阅了很多关于AQS的文章，讲到这个状态值的少之又少，哪怕是《Java并发编程实战》这本书，也是没有提及，最终我看到有一位博客园的作者非常详实的阐述了这个`PEOPAGATE`状态，也是给了我很大的启发。

没错，我第一次看AQS的源码的时候，甚至直接把这个`PROPAGATE`状态值忽略掉了。事实上，不仅仅阅读源码的人，容易把这个`PROPAGATE`状态值忽略掉，哪怕是Doug Lea老爷子本人，在开发时也没有意识到，如果没有这个状态值会导致什么样的后果，直到上面链接的bug出现后，老爷子才加上了这个状态，彻底修复了这个bug。

复现该bug的代码：

```java
import java.util.concurrent.Semaphore;

public class TestSemaphore {

    private static Semaphore sem = new Semaphore(0);

    private static class Thread1 extends Thread {
        @Override
        public void run() {
            sem.acquireUninterruptibly();
        }
    }

    private static class Thread2 extends Thread {
        @Override
        public void run() {
            sem.release();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i < 10000000; i++) {
            Thread t1 = new Thread1();
            Thread t2 = new Thread1();
            Thread t3 = new Thread2();
            Thread t4 = new Thread2();
            t1.start();
            t2.start();
            t3.start();
            t4.start();
            t1.join();
            t2.join();
            t3.join();
            t4.join();
            System.out.println(i);
        }
    }
}
复制代码
```

程序执行时，会偶发线程hang住。

我们再来看看之前的`setHeadAndPropagate`方法是什么样的。

```java
private void setHeadAndPropagate(Node node, int propagate) {
    setHead(node);
    if (propagate > 0 && node.waitStatus != 0) {
        Node s = node.next;
        if (s == null || s.isShared())
            unparkSuccessor(node);
    }
}
复制代码
```

然后Semaphore.release()调用的是AQS的`releaseShared`，看看当时的`releaseShared`长什么样：

```java
public final boolean releaseShared(int arg) {
    if (tryReleaseShared(arg)) {
        Node h = head;
        if (h != null && h.waitStatus != 0)
            unparkSuccessor(h);
        return true;
    }
    return false;
}
复制代码
```

再看看当时的Node：

```java
static final class Node {
    // 忽略掉无关的代码，只展示waitStatus的状态值

    static final int CANCELLED =  1;
    static final int SIGNAL    = -1;
    static final int CONDITION = -2;
}
复制代码
```

`setHeadAndPropagate`方法和`releaseShared`方法，设计的也是很简单。

当时源码里，Node的waitStatus是没有`PROPAGATE=-3`这个状态值的。

为了方便大家对照，我把当时`unparkSuccessor`方法的源码，也一并展示出来：

```java
private void unparkSuccessor(Node node) {

    // 将node的waitStatus设置为0
    compareAndSetWaitStatus(node, Node.SIGNAL, 0);

    Node s = node.next;
    if (s == null || s.waitStatus > 0) {
        s = null;
        for (Node t = tail; t != null && t != node; t = t.prev)
            if (t.waitStatus <= 0)
                s = t;
    }
    if (s != null)
        LockSupport.unpark(s.thread);
}
复制代码
```

接下来，我们慢慢聊～

> ps: 说真的，现在老板的位置离我的位置不远，虽然我的工作已经提前很多天完成了，但，还是有点慌～，冒着风险还要继续写！

在AQS获取共享锁的操作中，进入同步等待的线程（被阻塞掉），有两种途径可以被唤醒：

- 其他线程释放信号量后，调用unparkSuccessor（`releaseShared`方法中）
- 其他线程获取共享锁成功后，会通过传播机制来唤醒后继节点（也就是在`setHeadAndPropagate`方法中）。

bug重现的例子，很简单，就是在循环中重复不断的实例化4个线程，前两个线程获取信号量，两个线程释放信号量，主线程等待4个线程全都执行完毕再执行打印。

在后两个线程没有进行释放信号量的操作时，AQS内部的同步等待队列是下面这种情况：

```
+------+        +------+        +------+
|      |  <---  |      |  <---  |      |
| head |        |  t1  |        |  t2  |
|      |  --->  |      |  --->  |      |
+------+        +------+        +------+
复制代码
```

- 1、t3释放信号量，调用`releaseShared`，唤醒后继节点里的线程t1，同时，head的waitStatus变为0
- 2、t1被唤醒，调用Semaphore.NonfairSync的tryAcquireShared方法，返回0
- 3、t4释放信号量，调用`releaseShared`，在`releaseShared`方法中读到的head还是原head，但是此时head的waitStatus已经变为0，所以不会调用`unparkSuccessor`方法
- 4、t1被唤醒了，由于在步骤2里，调用Semaphore.NonfairSync的tryAcquireShared方法，返回的是0，所以它也不会调用`unparkSuccessor`方法

至此，两种途径全部被封死，没有任何线程去唤醒t2了，线程被hang住…

> ps：Doug Lea 黑人问号脸，哈哈～

老爷子为了修复这个bug，做出了如下改进：

- 1、增加一个waitStatus的状态，即`PROPAGATE`
- 2、在`releaseShared`方法中抽取提炼出了`doReleaseShared()`（上面有展示）在`doReleaseShared`方法中，如果head节点的状态为0，需要设置为PROPAGATE用以保证唤醒的传播。
- 3、在`setHeadAndPropagate`方法中也多了一些判断，其中就有head节点的waitStatus如果小于0，就唤醒后继节点（PROPAGATE = -3）。

通过改进之后的代码，我们再来复盘一下：

- 1、t3释放信号量，调用`releaseShared`，唤醒后继节点里的线程t1，同时，head的waitStatus变为0
- 2、t1被唤醒，调用Semaphore.NonfairSync的tryAcquireShared方法，返回0
- 3、此步骤和2和同一时刻发生，t4释放信号量，调用`releaseShared`，在`doReleaseShared`方法中读到的head还是原head，但是此时head的waitStatus已经变为0，将head的waitStatus设置为PROPAGATE（-3）
- 4、t1被唤醒了，调用`setHeadAndPropagate`方法，将t1设置为head，符合条件判断，进入分支语句，调用`doReleaseShared`方法，继而唤醒t2节点线程。

#### 6.2 unparkSuccessor的一点思考

```
private void unparkSuccessor(Node node) {
    int ws = node.waitStatus;
    if (ws < 0)
        compareAndSetWaitStatus(node, ws, 0);

    /*
     * 通常情况下，要唤醒的线程都是当前节点的后继线程
     * 但是，如果当前节点的后继节点被取消了，则从队列尾部向前遍历，直到找到未被取消的后继节点
     */
    Node s = node.next;
    if (s == null || s.waitStatus > 0) {
        s = null;
        for (Node t = tail; t != null && t != node; t = t.prev)
            if (t.waitStatus <= 0)
                s = t;
    }
    if (s != null)
        LockSupport.unpark(s.thread);
}
复制代码
```

unparkSuccessor方法中，如果当前节点的后继节点被取消了，则从队列尾部向前遍历，直到找到未被取消的后继节点。

这个问题，大家也可以自己思考一下，为什么要从tail节点开始向前遍历？

假设，CLH队列如下图所示：

```
+------+        +------+        +------+
|      |  <---  |      |  <---  |      |
| head |        |  t1  |        | tail |
|      |  --->  |      |  --->  |      |
+------+        +------+        +------+
复制代码
```

> t1.waitStatus = 1 且 tail.waitStatus = 1

head尝试唤醒后继节点t1，发现t1是被取消状态，遂找出t1的后继节点tail，发现tail也是被取消状态，但是tail.next == null。

与此同时，有个新节点加入到队列尾部，但是还没有将原tail.next指向新节点。

也就是说，tail.next 如果恰好处在步骤1和步骤2中间的话，遍历就会中断。

摘录addWaiter部分代码：

```java
node.prev = pred;
// 通过CAS在队尾插入当前节点
if (compareAndSetTail(pred, node)) { // 步骤1
    pred.next = node; // 步骤2
    return node;
}
复制代码
```

#### 6.3 acquireQueued 方法里，为什么还要再tryAcquire？

以独占模式来说，对于这个问题，我是这么想的：

时刻1：线程B尝试获取锁，但是，由于锁被线程A持有，所以，线程B准备调用`addWaiter`，将自己入到队列（但还没有和head节点产生指针连接）

时刻1：同一时刻，线程A尝试释放锁，进入release方法，调用子类的tryRelease()，将代表锁持有次数的state置为0（代表锁没有被任何线程持有），进入`unparkSuccessor`方法，发现并没有后继节点（因为新节点还未入队），所以不会唤醒任何线程，到这里，线程A释放锁操作完成。

时刻2：线程B调用`addWaiter`方法完毕，已经入队，并和head节点产生指针连接

时刻3：线程B调用`acquireQueued`方法（如下方代码展示），如果在这个方法里面不调用`tryAcquire`，就会发生这样的情况：**明明可以获取锁，但是线程却被休眠了，进而导致整个同步队列不可用**

所以，**再次调用tryAcquire是为了防止新节点还未入队，但是头结点已经释放了锁，导致整个同步队列瘫痪的情况发生。**

```java
final boolean acquireQueued(final Node node, int arg) {
    boolean failed = true;
    try {
        boolean interrupted = false;
        for (;;) {
            final Node p = node.predecessor();
            // 防止新节点还未入队，但是头结点已经释放了锁，导致整个同步队列中断瘫痪的情况发生
            if (p == head && tryAcquire(arg)) {
                setHead(node);
                p.next = null; // help GC
                failed = false;
                return interrupted;
            }
            if (shouldParkAfterFailedAcquire(p, node) && parkAndCheckInterrupt())
                interrupted = true;
        }
    } finally {
        if (failed)
            cancelAcquire(node);
    }
}

```

### 结束

通过阅读AQS的源码，对于我们学习和掌握基于AQS实现的组件，是有很大帮助的。

尤其是它的设计理念和思想，更是我们学习的重点！

