# Dubbo-SPI

### 自己实现一套SPI的原因

- JDK标准的SPI一次性实例化所有的扩展点，容易浪费资源。
  - ExtensionLoader采用两个Map，分别存储扩展点的类以及其实例
- JDK的SPI扩展点加载失败，连扩展名都不无法获取
- Dubbo SPI增加对扩展点IoC与AOP的支持，一个扩展点可以直接setter注入到其他的扩展点中。
  - ExtensionLoader中的injectExtension方法，通过反射实现类所有的setter方法判定其成员注入。
  - 注入依赖扩展点有多个实现的时候，  通过扩展点自适应(Adaptive)机制决定注入具体的对象。

### 核心代码

#### ExtensionLoader 

```java
private static final String SERVICES_DIRECTORY = "META-INF/services/";
private static final String DUBBO_DIRECTORY = "META-INF/dubbo/";
private static final String DUBBO_INTERNAL_DIRECTORY = DUBBO_DIRECTORY + "internal/";
private static final Pattern NAME_SEPARATOR = Pattern.compile("\\s*[,]+\\s*");
// 扩展加载器集合
private static final ConcurrentMap<Class<?>, ExtensionLoader<?>> EXTENSION_LOADERS = new ConcurrentHashMap<>();
// 扩展实现类与实例集合
private static final ConcurrentMap<Class<?>, Object> EXTENSION_INSTANCES = new ConcurrentHashMap<>();
//======================对象属性=========================
// 扩展接口，例如 Protocol
private final Class<?> type;
/**
  * 对象工厂
  * 用于调用 {@link #injectExtension(Object)} 方法，向拓展对象注入依赖属性。
  * 例如，StubProxyFactoryWrapper 中有 `Protocol protocol` 属性。
*/
private final ExtensionFactory objectFactory;
//缓存的扩展名与扩展类的映射
private final ConcurrentMap<Class<?>, String> cachedNames = new ConcurrentHashMap<>();
/**缓存的扩展实现类集合
  *不包括以下两种类型：
  *1：自适应扩展实现类。例如 AdaptiveExtensionFactory
  *2：带唯一参数为扩展接口的构造方法的实现类，或者扩展Wrapper实现类。例如,ProtocolFilterWrapper。
  *  拓展 Wrapper 实现类，会添加到 {@link #cachedWrapperClasses} 中
  *  通过 {@link #loadExtensionClasses} 加载
*/
private final Holder<Map<String, Class<?>>> cachedClasses = new Holder<>();
/**
  * 拓展名与 @Activate 的映射
  * 例如，AccessLogFilter。
  * 用于 {@link #getActivateExtension(URL, String)}
*/
private final Map<String, Object> cachedActivates = new ConcurrentHashMap<>();
/**
  * 缓存的拓展对象集合
  * key：拓展名
  * value：拓展对象
  * 例如，Protocol 拓展
  *      key：dubbo value：DubboProtocol
  *      key：injvm value：InjvmProtocol
  * 通过 {@link #loadExtensionClasses} 加载
*/
private final ConcurrentMap<String, Holder<Object>> cachedInstances = new ConcurrentHashMap<>();
/**
  * 缓存的自适应( Adaptive )拓展对象
*/
private final Holder<Object> cachedAdaptiveInstance = new Holder<>();
/**
  * 缓存的自适应拓展对象的类
  * {@link #getAdaptiveExtensionClass()}
*/
private volatile Class<?> cachedAdaptiveClass = null;
/**
  * 缓存的默认拓展名
  * 通过 {@link SPI} 注解获得
*/
private String cachedDefaultName;
/**
  * 创建 {@link #cachedAdaptiveInstance} 时发生的异常。
  * 发生异常后，不再创建，参见 {@link #createAdaptiveExtension()}
*/
private volatile Throwable createAdaptiveInstanceError;
/**
  * 拓展 Wrapper 实现类集合
  * 带唯一参数为拓展接口的构造方法的实现类
  * 通过 {@link #loadExtensionClasses} 加载
*/
private Set<Class<?>> cachedWrapperClasses;
/**
  * 拓展名 与 加载对应拓展类发生的异常 的 映射
  * 在 {@link #loadFile(Map, String)} 时，记录
*/
private Map<String, IllegalStateException> exceptions = new ConcurrentHashMap<>();
```

- 【静态属性】
  - ExtensionLoader是ExtensionLoader的管理容器。
  - 一个扩展(扩展接口)对应一个ExtensionLoader对象。例如Protocol与Filter分别对一个ExtensionLoader对象。
- 【对象属性】
  - 一个扩展通过其ExtensionLoader实例，加载它的所有扩展实现们。出于性能与资源优化，ExtensionLoader读取配置之后，先进行缓存，等真正需要的时候，Dubbo才进行ExtensionLoader实现类的对象初始化，并将对象缓存。
    - 缓存加载的扩展配置。
    - 缓存创建的扩展实现对象。
- 扩展点采用单例加载，需要注意线程安全问题。

##### Wrapper

- Wrapper类同样实现扩展点接口，但是不是真正的实现。
  - 从ExtensionLoader返回扩展点时，Wrapper持有了实际的扩展点实现类。
  - Wrapper代理扩展点。

#### 扩展点自适应

- ExtensionLoader注入的依赖扩展点是一个@Adaptive实例，直到扩展点方法执行时才决定调用是一个扩展点实现。
- Dubbo使用URL对象（包含了key-value）传递配置信息。
- Dubbo的ExtensionLoader的扩展点类对应的Adaptive实现是在加载扩展点里动态生成。指定提取的URL的Key通过@Adaptive注解在接口方法上提供。
- @Adaptive激活
  - @Activate  //无条件自动激活
  - @Activate("xx")  //当配置了xx参数，且参数为有效值时激活
  - @Activate(group="provider", value="xx") //只对提供方激活，group可选“provider” 或“consumer”



